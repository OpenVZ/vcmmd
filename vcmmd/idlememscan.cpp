#include <Python.h>

#include <fstream>
#include <sstream>
#include <string>
#include <utility>
#include <algorithm>
#include <unordered_map>

#include <sys/mman.h>
#include <linux/kernel-page-flags.h>

#define ZONEINFO_PATH		"/proc/zoneinfo"

#define KPAGEFLAGS_PATH		"/proc/kpageflags"
#define KPAGECGROUP_PATH	"/proc/kpagecgroup"
#define IDLE_PAGE_BITMAP_PATH	"/sys/kernel/mm/page_idle/bitmap"

// must be multiple of 64 for the sake of idle page bitmap
#define BATCH_SIZE		1024

// how many pages py_iter scans in one go
#define SCAN_CHUNK		32768

using namespace std;

class error: public exception {
private:
	string msg_;
public:
	error(const string &msg) : msg_(msg) { }
	virtual const char *what() const throw() { return msg_.c_str(); }
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

// Converts caught exception to appropriate PyErr
#define py_catch_error()						\
	catch (error &e) {						\
		return PyErr_Format(PyExc_RuntimeError, "%s", e.what());\
	} catch (bad_alloc e) {						\
		return PyErr_NoMemory();				\
	}

// The following constant must fit in char, because we want to have only one
// extra byte per tracked page for storing age. We could use 4 bits or even 2
// bits, so that 2 or 4 pages would share the same byte, but it would
// complicate the code and shorten the history significantly.
#define MAX_IDLE_AGE		255

static long END_PFN;
static unsigned char *idle_page_age;

#define IDLE_STAT_BUCKETS	(MAX_IDLE_AGE + 1)

// bucket i (0 <= i < 255) -> nr idle for exactly (i + 1) intervals
// bucket 255 -> nr idle for >= last 256 intervals
struct idle_stat_buckets_array {
	long count[IDLE_STAT_BUCKETS];
};

enum mem_type {
	MEM_ANON,
	MEM_FILE,
	NR_MEM_TYPES,
};

class idle_mem_stat {
private:
	long total_[NR_MEM_TYPES];
	idle_stat_buckets_array idle_[NR_MEM_TYPES];
public:
	idle_mem_stat()
	{
		for (int i = 0; i < NR_MEM_TYPES; ++i) {
			total_[i] = 0;
			for (int j = 0; j < IDLE_STAT_BUCKETS; j++)
				idle_[i].count[j] = 0;
		}
	}

	// idle_by_age[i] equals nr pages that have been idle for > i intervals
	// (cf. idle_stat_buckets_array)
	void get_nr_idle(mem_type type, long idle_by_age[IDLE_STAT_BUCKETS])
	{
		long sum = 0;

		for (int i = IDLE_STAT_BUCKETS - 1; i >= 0; i--) {
			sum += idle_[type].count[i];
			idle_by_age[i] = sum;
		}
	}

	void inc_nr_idle(mem_type type, int age)
	{
		++idle_[type].count[age];
	}

	long get_nr_total(mem_type type)
	{
		return total_[type];
	}

	void inc_nr_total(mem_type type)
	{
		++total_[type];
	}
};

// ino -> idle_mem_stat
static unordered_map<long, class idle_mem_stat> cg_idle_mem_stat;

static void do_open(const char *path, ios_base::openmode mode,
		    long pos, fstream &f) throw(error)
{
	// disable stream buffering - we know better how to do it
	f.rdbuf()->pubsetbuf(0, 0);

	f.open(path, mode | ios::binary);
	if (!f)
		throw error(string("Open '") + path + "' failed");

	// seek to the requested position
	f.seekg(pos * 8);
}

static void do_read(fstream &f, int n, const char *path,
		     uint64_t *buf) throw(error)
{
	if (!f.read(reinterpret_cast<char *>(buf), n * 8))
		throw error(string("Read '") + path + "' failed");

}

static void do_write(fstream &f, int n, const char *path,
		     const uint64_t *buf) throw(error)
{
	if (!f.write(reinterpret_cast<const char *>(buf), n * 8))
		throw error(string("Write '") + path + "' failed");

}

// Marks pages in range [start_pfn, end_pfn) idle.
static void set_idle_pages(long start_pfn, long end_pfn) throw(error)
{
	// idle page bitmap requires pfn to be aligned by 64
	long start_pfn2 = start_pfn & ~63UL;
	long end_pfn2 = (end_pfn + 63) & ~63UL;

	fstream f;
	do_open(IDLE_PAGE_BITMAP_PATH, ios::out, start_pfn2 / 64, f);

	uint64_t buf[BATCH_SIZE / 64];
	for (int i = 0; i < BATCH_SIZE / 64; i++)
		buf[i] = ~0ULL;

	for (long pfn = start_pfn2; pfn < end_pfn; pfn += BATCH_SIZE) {
		int n = min((long)BATCH_SIZE, end_pfn2 - pfn);
		buf[0] = buf[n / 64 - 1] = ~0ULL;
		if (pfn < start_pfn)
			buf[0] &= ~((1ULL << (start_pfn & 63)) - 1);
		if (pfn + n > end_pfn)
			buf[n / 64 - 1] &= (1ULL << (end_pfn & 63)) - 1;
		do_write(f, n / 64, IDLE_PAGE_BITMAP_PATH, buf);
	}
}

// Counts idle pages in range [start_pfn, end_pfn).
// Returns map: cg ino -> idle_mem_stat.
static void count_idle_pages(long start_pfn, long end_pfn) throw(error)
{
	// idle page bitmap requires pfn to be aligned by 64
	long start_pfn2 = start_pfn & ~63UL;
	long end_pfn2 = (end_pfn + 63) & ~63UL;

	fstream f_flags, f_cg, f_idle;
	do_open(KPAGEFLAGS_PATH, ios::in, start_pfn2, f_flags);
	do_open(KPAGECGROUP_PATH, ios::in, start_pfn2, f_cg);
	do_open(IDLE_PAGE_BITMAP_PATH, ios::in, start_pfn2 / 64, f_idle);

	uint64_t buf_flags[BATCH_SIZE],
		 buf_cg[BATCH_SIZE],
		 buf_idle[BATCH_SIZE / 64];

	bool head_idle = false, head_anon = false, head_lru = false;
	long head_cg = 0;
	int buf_index = BATCH_SIZE;

	for (long pfn = start_pfn2; pfn < end_pfn; ++pfn, ++buf_index) {
		if (buf_index >= BATCH_SIZE) {
			// buffer is empty - refill
			int n = min((long)BATCH_SIZE, end_pfn2 - pfn);
			do_read(f_flags, n, KPAGEFLAGS_PATH, buf_flags);
			do_read(f_cg, n, KPAGECGROUP_PATH, buf_cg);
			do_read(f_idle, n / 64, IDLE_PAGE_BITMAP_PATH,
				buf_idle);
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
			head_idle = buf_idle[buf_index / 64] &
					(1ULL << (buf_index & 63));

			// do not treat mlock'd pages as idle
			if (flags & (1 << KPF_UNEVICTABLE))
				head_idle = false;
		} // else compound page tail - count as per head

		if (!head_lru)
			continue;

		auto &stat = cg_idle_mem_stat[head_cg];
		mem_type type = head_anon ? MEM_ANON : MEM_FILE;

		stat.inc_nr_total(type);

		if (head_idle) {
			int age = idle_page_age[pfn];
			if (age < MAX_IDLE_AGE)
				idle_page_age[pfn] = age + 1;
			stat.inc_nr_idle(type, age);
		} else
			idle_page_age[pfn] = 0;

	}
}

static PyObject *py_nr_iters(PyObject *self, PyObject *args)
{
	int nr_iters = (END_PFN + SCAN_CHUNK - 1) / SCAN_CHUNK;
	PyObject *ret = PyInt_FromLong(nr_iters);
	if (!ret)
		return PyErr_NoMemory();
	return ret;
}

// Does one scan iter. Returns true if the current scan was finished.
static PyObject *py_iter(PyObject *self, PyObject *args)
{
	static int scan_iter;
	bool ret = false;

	if (!scan_iter)
		cg_idle_mem_stat.clear();

	long start_pfn = scan_iter * SCAN_CHUNK;
	long end_pfn = start_pfn + SCAN_CHUNK;
	if (end_pfn >= END_PFN) {
		end_pfn = END_PFN;
		scan_iter = 0;
		ret = true;
	} else
		scan_iter++;

	try {
		count_idle_pages(start_pfn, end_pfn);
		set_idle_pages(start_pfn, end_pfn);
	} py_catch_error();

	if (ret)
		Py_RETURN_TRUE;
	else
		Py_RETURN_FALSE;
}

// Returns dict: cg ino -> (anon stats, file stats).
//
// Anon/file stats are represented by tuple:
//
// (total, idle[1], idle[2], ..., idle[IDLE_STAT_BUCKETS])
//
// where @total is the total number of ageable pages scanned, @idle[i] is the
// number of pages that have been idle for >= i last intervals.
static PyObject *py_result(PyObject *self, PyObject *args)
{
	// map the result to a PyDict
	py_ref dict = PyDict_New();
	if (!dict)
		return PyErr_NoMemory();

	for (auto &kv : cg_idle_mem_stat) {
		py_ref key = PyInt_FromLong(kv.first);
		py_ref val = PyTuple_New(NR_MEM_TYPES);
		if (!key || !val)
			return PyErr_NoMemory();

		for (int i = 0; i < NR_MEM_TYPES; i++) {
			mem_type t = static_cast<mem_type>(i);

			long idle_stat[IDLE_STAT_BUCKETS + 1];
			idle_stat[0] = kv.second.get_nr_total(t);
			kv.second.get_nr_idle(t, idle_stat + 1);

			py_ref idle_stat_tuple =
				PyTuple_New(IDLE_STAT_BUCKETS + 1);
			if (!idle_stat_tuple)
				return PyErr_NoMemory();

			for (int j = 0; j <= IDLE_STAT_BUCKETS; j++) {
				PyObject *p = PyInt_FromLong(idle_stat[j]);
				if (!p)
					return PyErr_NoMemory();
				PyTuple_SET_ITEM(static_cast<PyObject *>
						 (idle_stat_tuple), j, p);
			}

			// PyTuple_SET_ITEM steals reference
			Py_INCREF(idle_stat_tuple);
			PyTuple_SET_ITEM(static_cast<PyObject *>(val),
					 i, idle_stat_tuple);
		}

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
		"nr_iters",
		(PyCFunction)py_nr_iters,
		METH_NOARGS, NULL,
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

static void init_idle_page_age_array()
{
	idle_page_age = (unsigned char *)mmap(NULL, END_PFN,
			PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
	if (!idle_page_age)
		throw error("Failed to allocate idle_page_age array");
}

PyMODINIT_FUNC
initidlememscan(void)
{
	init_END_PFN();
	init_idle_page_age_array();

	PyObject *m = Py_InitModule("idlememscan", idlememscan_funcs);
	if (!m)
		return;

	PyModule_AddIntConstant(m, "MAX_AGE", MAX_IDLE_AGE + 1);
}
