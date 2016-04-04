#include <Python.h>

#include <fstream>
#include <sstream>
#include <string>
#include <utility>
#include <algorithm>
#include <unordered_map>

#include <unistd.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <linux/kernel-page-flags.h>

#define ZONEINFO_PATH		"/proc/zoneinfo"

#define KPAGEFLAGS_PATH		"/proc/kpageflags"
#define KPAGECGROUP_PATH	"/proc/kpagecgroup"
#define IDLE_PAGE_BITMAP_PATH	"/sys/kernel/mm/page_idle/bitmap"

#define MEM_CGROUP_ROOT_PATH	"/sys/fs/cgroup/memory"

// must be multiple of 64 for the sake of idle page bitmap
#define BATCH_SIZE		4096

// how many pages py_iter scans in one go
#define SCAN_CHUNK		32768

using namespace std;

class error: public exception {
private:
	string msg_;
public:
	error(const string &msg) : msg_(msg) { }
	virtual const char *what() const throw() { return msg_.c_str(); }
	void set_py_err() { PyErr_SetString(PyExc_RuntimeError, this->what()); }
};

// With this class we do not need to bother about dropping ref to a PyObject -
// it is dropped automatically in destructor.
class py_ref {
private:
	PyObject *obj_;
public:
	py_ref(PyObject *obj) : obj_(obj) { }
	~py_ref() { Py_XDECREF(obj_); }
	operator PyObject *() const { return obj_; }
	operator bool() const { return !!obj_; }
};

static long END_PFN;

// scan 1/sampling pages
static int sampling = 1;

// how many pages one iteration spans
static int iter_span = SCAN_CHUNK;

enum mem_type {
	MEM_ANON,
	MEM_FILE,
	NR_MEM_TYPES,
};

class idle_mem_stat {
private:
	long total_[NR_MEM_TYPES];
	long idle_[NR_MEM_TYPES];
public:
	idle_mem_stat()
	{
		for (int i = 0; i < NR_MEM_TYPES; ++i) {
			total_[i] = 0;
			idle_[i] = 0;
		}
	}

	long get_nr_total(mem_type type)
	{
		return total_[type];
	}

	long get_nr_idle(mem_type type)
	{
		return idle_[type];
	}

	void inc_nr_total(mem_type type)
	{
		++total_[type];
	}

	void inc_nr_idle(mem_type type)
	{
		++idle_[type];
	}

	idle_mem_stat &operator +=(const idle_mem_stat &other)
	{
		for (int i = 0; i < NR_MEM_TYPES; ++i) {
			total_[i] += other.total_[i];
			idle_[i] += other.idle_[i];
		}
		return *this;
	}
};

// ino -> idle_mem_stat
static unordered_map<long, idle_mem_stat> cg_idle_mem_stat;

// /proc/kpageflags, /proc/kpagecgroup, /sys/kernel/mm/page_idle/bitmap
static fstream f_flags, f_cg, f_idle;

static void do_open(const char *path, ios_base::openmode mode,
		    fstream &f) throw(error)
{
	// disable stream buffering - we know better how to do it
	f.rdbuf()->pubsetbuf(0, 0);

	f.open(path, mode | ios::binary);
	if (!f)
		throw error(string("Open '") + path + "' failed");
}

static void throw_rw_error(const char *path, bool write,
			   long off, long sz) throw(error)
{
	ostringstream ss;
	ss << (write ? "Write" : "Read") << " '" << path << "' " <<
		sz << '@' << off << " failed";
	throw error(ss.str());
}

static void do_read(fstream &f, long pos, int n, const char *path,
		     uint64_t *buf) throw(error)
{
	f.seekg(pos * 8);
	if (!f.read(reinterpret_cast<char *>(buf), n * 8))
		throw_rw_error(path, false, pos * 8, n * 8);

}

static void do_write(fstream &f, long pos, int n, const char *path,
		     const uint64_t *buf) throw(error)
{
	f.seekg(pos * 8);
	if (!f.write(reinterpret_cast<const char *>(buf), n * 8))
		throw_rw_error(path, true, pos * 8, n * 8);

}

static void open_files()
{
	static bool opened;

	if (opened)
		return;

	do_open(KPAGEFLAGS_PATH, ios::in, f_flags);
	do_open(KPAGECGROUP_PATH, ios::in, f_cg);
	do_open(IDLE_PAGE_BITMAP_PATH, ios::in | ios::out, f_idle);

	opened = true;
}

// Marks pages in range [start_pfn, end_pfn) idle.
static void set_idle_pages(long start_pfn, long end_pfn) throw(error)
{
	// idle page bitmap requires pfn to be aligned by 64
	long start_pfn2 = start_pfn & ~63UL;
	long end_pfn2 = (end_pfn + 63) & ~63UL;

	uint64_t buf[BATCH_SIZE / 64];
	for (int i = 0; i < BATCH_SIZE / 64; i++)
		buf[i] = ~0ULL;

	for (long pfn = start_pfn2; pfn < end_pfn;
	     pfn += BATCH_SIZE * sampling) {
		int n = min((long)BATCH_SIZE, end_pfn2 - pfn);
		buf[0] = buf[n / 64 - 1] = ~0ULL;
		if (pfn < start_pfn)
			buf[0] &= ~((1ULL << (start_pfn & 63)) - 1);
		if (pfn + n > end_pfn)
			buf[n / 64 - 1] &= (1ULL << (end_pfn & 63)) - 1;
		do_write(f_idle, pfn / 64, n / 64, IDLE_PAGE_BITMAP_PATH, buf);
	}
}

static inline long __next_pfn(long pfn, long buf_index)
{
	if (buf_index >= BATCH_SIZE)
		pfn += BATCH_SIZE * (sampling - 1);
	return pfn + 1;
}

// Counts idle pages in range [start_pfn, end_pfn).
// Returns map: cg ino -> idle_mem_stat.
static void count_idle_pages(long start_pfn, long end_pfn) throw(error)
{
	uint64_t buf_flags[BATCH_SIZE],
		 buf_cg[BATCH_SIZE],
		 buf_idle[BATCH_SIZE / 64];

	long head_cg = 0;
	bool head_lru = false, head_anon = false,
	     head_unevictable = false, head_idle = false;
	int buf_index = BATCH_SIZE;

	// idle page bitmap requires pfn to be aligned by 64
	for (long pfn = (start_pfn & ~63UL); pfn < end_pfn;
	     pfn = __next_pfn(pfn, ++buf_index)) {
		if (buf_index >= BATCH_SIZE) {
			// buffer is empty - refill
			int n = min((long)BATCH_SIZE, end_pfn - pfn);
			do_read(f_flags, pfn, n, KPAGEFLAGS_PATH, buf_flags);
			do_read(f_cg, pfn, n, KPAGECGROUP_PATH, buf_cg);
			do_read(f_idle, pfn / 64, (n + 63) / 64,
				IDLE_PAGE_BITMAP_PATH, buf_idle);
			buf_index = 0;
		}

		if (pfn < start_pfn)
			continue;

		uint64_t flags = buf_flags[buf_index],
			 cg = buf_cg[buf_index];

		if (!(flags & (1 << KPF_COMPOUND_TAIL))) {
			// not compound page or compound page head
			head_cg = cg;
			head_lru = !!(flags & (1 << KPF_LRU));
			head_anon = !!(flags & (1 << KPF_ANON));
			head_unevictable = !!(flags & (1 << KPF_UNEVICTABLE));
			head_idle = buf_idle[buf_index / 64] &
					(1ULL << (buf_index & 63));
		} // else compound page tail - count as per head

		if (!head_lru || head_unevictable)
			continue;

		auto &stat = cg_idle_mem_stat[head_cg];
		mem_type type = head_anon ? MEM_ANON : MEM_FILE;

		stat.inc_nr_total(type);
		if (head_idle)
			stat.inc_nr_idle(type);
	}
}

static PyObject *py_set_sampling(PyObject *self, PyObject *args)
{
	double value;

	if (!PyArg_ParseTuple(args, "d", &value))
		return NULL;

	if (value <= 0 || value > 1) {
		PyErr_SetString(PyExc_ValueError,
				"argument must be in range (0.0, 1.0]");
		return NULL;
	}

	// inverse ratio fits better for calculations
	sampling = max(static_cast<int>(1 / value), 1);

	iter_span = SCAN_CHUNK * sampling;

	Py_RETURN_NONE;
}

// Does one scan iteration. Returns a tuple containing the total number of
// iterations done and left to do for the current scan.
static PyObject *py_iter(PyObject *self, PyObject *args)
{
	static int cur_iter;

	if (!cur_iter)
		cg_idle_mem_stat.clear();

	long start_pfn = cur_iter * iter_span;
	long end_pfn = start_pfn + iter_span;
	if (end_pfn >= END_PFN)
		end_pfn = END_PFN;

	int iters_left = (END_PFN - end_pfn + iter_span - 1) / iter_span;
	cur_iter++;

	try {
		open_files();
		count_idle_pages(start_pfn, end_pfn);
		set_idle_pages(start_pfn, end_pfn);
	} catch (error &e) {
		e.set_py_err();
		return NULL;
	}

	PyObject *ret = Py_BuildValue("(ii)",  cur_iter, iters_left);
	if (!ret)
		return PyErr_NoMemory();

	if (iters_left == 0)
		cur_iter = 0;

	return ret;
}

static idle_mem_stat &__get_result(const string &path, ino_t ino,
				   unordered_map<string, idle_mem_stat> &result)
{
	DIR *d = opendir((MEM_CGROUP_ROOT_PATH + path).c_str());
	if (!d)
		throw error(string("Failed to read dir '") + path + '\'');

	auto &my_result = result[path];
	if (ino) // not interested in root
		my_result = cg_idle_mem_stat[ino];

	dirent *entry;
	while ((entry = readdir(d)) != NULL) {
		// we are only interested in cgroup directories
		if (!(entry->d_type & DT_DIR))
			continue;

		// filter out . and ..
		if (entry->d_name[0] == '.') {
			if (entry->d_name[1] == '\0')
				continue;
			if (entry->d_name[1] == '.' &&
			    entry->d_name[2] == '\0')
				continue;
		}

		string child_path = path;
		if (ino)
			child_path += '/';
		child_path += entry->d_name;

		auto &child_result = __get_result(child_path,
						  entry->d_ino, result);
		if (ino) // not interested in root
			my_result += child_result;
	}
	closedir(d);
	return my_result;
}

static unordered_map<string, idle_mem_stat> get_result()
{
	unordered_map<string, idle_mem_stat> result;
	__get_result("/", 0, result);
	result.erase("/"); // not interested in root
	return result;
}

// Returns dict: cg path -> idle stats.
//
// Idle stats are represented by a tuple:
//
//   (total_anon, idle_anon, total_file, idle_file)
//
// where @total_{anon,file} is the total number of anon/file pages scanned,
// @idle_{anon,file} is the number of anon/file pages that have not been
// touched since the last scan.
static PyObject *py_result(PyObject *self, PyObject *args)
{
	// map the result to a PyDict
	py_ref dict = PyDict_New();
	if (!dict)
		return PyErr_NoMemory();

	auto result = get_result();
	for (auto &kv : result) {
		py_ref key = PyString_FromString(kv.first.c_str());
		if (!key)
			return PyErr_NoMemory();

		py_ref val = Py_BuildValue("(llll)",
					   kv.second.get_nr_total(MEM_ANON),
					   kv.second.get_nr_idle(MEM_ANON),
					   kv.second.get_nr_total(MEM_FILE),
					   kv.second.get_nr_idle(MEM_FILE));
		if (!val)
			return PyErr_NoMemory();

		if (PyDict_SetItem(dict, key, val) < 0)
			return PyErr_NoMemory();
	}

	// the dict reference we are holding now will be dropped by ~py_ref(),
	// so we have to take one more reference before returning it
	Py_INCREF(dict);
	return dict;
}

static PyMethodDef idlememscan_funcs[] = {
	{
		"set_sampling",
		(PyCFunction)py_set_sampling,
		METH_VARARGS, NULL,
	},
	{
		"iter",
		(PyCFunction)py_iter,
		METH_NOARGS, NULL,
	},
	{
		"result",
		(PyCFunction)py_result,
		METH_NOARGS, NULL,
	},
	{ },
};

static void init_END_PFN()
{
	fstream f(ZONEINFO_PATH, ios::in);
	string line;
	long spanned = 0;
	while (getline(f, line)) {
		stringstream ss(line);
		string key;
		ss >> key;
		if (key == "spanned") {
			ss >> spanned;
		} else if (key == "start_pfn:") {
			long pfn;
			ss >> pfn;
			pfn += spanned;
			spanned = 0;
			if (pfn > END_PFN)
				END_PFN = pfn;
		}
	}
	if (END_PFN == 0)
		throw error("Failed to parse zoneinfo");
}

PyMODINIT_FUNC
initidlememscan(void)
{
	try {
		init_END_PFN();
	} catch (error &e) {
		e.set_py_err();
		return;
	}

	Py_InitModule("idlememscan", idlememscan_funcs);
}
