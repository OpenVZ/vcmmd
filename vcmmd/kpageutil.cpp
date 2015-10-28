#include <Python.h>

#include <fstream>
#include <string>
#include <utility>
#include <algorithm>
#include <unordered_map>

#include <linux/kernel-page-flags.h>

#define KPAGEFLAGS_PATH		"/proc/kpageflags"
#define KPAGECGROUP_PATH	"/proc/kpagecgroup"
#define IDLE_PAGE_BITMAP_PATH	"/sys/kernel/mm/page_idle/bitmap"

// must be multiple of 64 for the sake of idle page bitmap
#define BATCH_SIZE		1024

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

enum mem_type {
	MEM_ANON,
	MEM_FILE,
	NR_MEM_TYPES,
};

class idle_mem_stat {
private:
	long idle_[NR_MEM_TYPES];
public:
	idle_mem_stat()
	{
		for (int i = 0; i < NR_MEM_TYPES; ++i)
			idle_[i] = 0;
	}

	long get_nr_idle(mem_type type)
	{
		return idle_[type];
	}

	void inc_nr_idle(mem_type type)
	{
		++idle_[type];
	}
};

// ino -> idle_mem_stat
typedef unordered_map<long, class idle_mem_stat> cg_idle_mem_stat_t;

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
cg_idle_mem_stat_t count_idle_pages(long start_pfn, long end_pfn) throw(error)
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

	bool head_idle = false, head_anon = false;
	long head_cg = 0;
	int buf_index = BATCH_SIZE;

	cg_idle_mem_stat_t result;

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
		bool idle = buf_idle[buf_index / 64] & (1ULL << (buf_index & 63));

		if (!(flags & (1 << KPF_COMPOUND_TAIL))) {
			// not compound page or compound page head
			head_idle = false;
			head_cg = cg;
			head_anon = !!(flags & (1 << KPF_ANON));

			if (!idle)
				continue;

			// do not treat mlock'd pages as idle
			if (flags & (1 << KPF_UNEVICTABLE))
				continue;

			head_idle = true;
		} else {
			// compound page tail - count it if the head is idle
			if (!head_idle)
				continue;
		}

		auto &stat = result[head_cg];
		if (head_anon)
			stat.inc_nr_idle(MEM_ANON);
		else
			stat.inc_nr_idle(MEM_FILE);
	}
	return result;
}

static PyObject *py_set_idle_pages(PyObject *self, PyObject *args)
{
	long start_pfn, end_pfn;
	if (!PyArg_ParseTuple(args, "ll", &start_pfn, &end_pfn))
		return NULL;

	try {
		set_idle_pages(start_pfn, end_pfn);
	} py_catch_error();

	Py_RETURN_NONE;
}

// Returns dict: cg ino -> (idle anon, idle file).
static PyObject *py_count_idle_pages(PyObject *self, PyObject *args)
{
	long start_pfn, end_pfn;
	if (!PyArg_ParseTuple(args, "ll", &start_pfn, &end_pfn))
		return NULL;

	cg_idle_mem_stat_t result;
	try {
		result = count_idle_pages(start_pfn, end_pfn);
	} py_catch_error();

	// map the result to a PyDict
	py_ref dict = PyDict_New();
	if (!dict)
		return PyErr_NoMemory();

	for (auto &kv : result) {
		py_ref key = PyInt_FromLong(kv.first);
		py_ref val = PyTuple_New(NR_MEM_TYPES);
		if (!key || !val)
			return PyErr_NoMemory();

		for (int i = 0; i < NR_MEM_TYPES; i++) {
			mem_type t = static_cast<mem_type>(i);
			PyObject *p = PyInt_FromLong(kv.second.get_nr_idle(t));
			if (!p)
				return PyErr_NoMemory();
			PyTuple_SET_ITEM((PyObject *)val, i, p);
		}

		if (PyDict_SetItem(dict, key, val) < 0)
			return PyErr_NoMemory();
	}

	// the dict reference we are holding now will be dropped by ~py_ref(),
	// so we have to take one more reference before returning it
	Py_INCREF(dict);
	return dict;
}

static PyMethodDef kpageutil_funcs[] = {
	{
		"set_idle_pages",
		(PyCFunction)py_set_idle_pages,
		METH_VARARGS, NULL,
	},
	{
		"count_idle_pages",
		(PyCFunction)py_count_idle_pages,
		METH_VARARGS, NULL,
	},
	{ },
};

extern "C" {
void initkpageutil(void)
{
	Py_InitModule("kpageutil", kpageutil_funcs);
}
}
