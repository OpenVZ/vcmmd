#include <Python.h>

#include <fstream>
#include <string>
#include <utility>
#include <algorithm>
#include <unordered_map>

#include <linux/kernel-page-flags.h>

#define KPAGEFLAGS_PATH		"/proc/kpageflags"
#define KPAGECGROUP_PATH	"/proc/kpagecgroup"
#define KPAGEIDLE_PATH		"/proc/kpageidle"

// must be multiple of 64 for the sake of kpageidle
#define KPAGE_BATCH		1024

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

// Helper for opening /proc/kpage*
static void kpf_open(const char *path, ios_base::openmode mode,
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

// Helper for reading /proc/kpage*
static void kpf_read(fstream &f, int n, const char *path,
		     uint64_t *buf) throw(error)
{
	if (!f.read(reinterpret_cast<char *>(buf), n * 8))
		throw error(string("Read '") + path + "' failed");

}

// Helper for writing /proc/kpage*
static void kpf_write(fstream &f, int n, const char *path,
		      const uint64_t *buf) throw(error)
{
	if (!f.write(reinterpret_cast<const char *>(buf), n * 8))
		throw error(string("Write '") + path + "' failed");

}

// Marks pages in range [start_pfn, end_pfn) idle.
static void set_idle_pages(long start_pfn, long end_pfn) throw(error)
{
	// kpageidle requires pfn to be aligned by 64
	long start_pfn2 = start_pfn & ~63UL;
	long end_pfn2 = (end_pfn + 63) & ~63UL;

	fstream f;
	kpf_open(KPAGEIDLE_PATH, ios::out, start_pfn2 / 64, f);

	uint64_t buf[KPAGE_BATCH / 64];
	for (int i = 0; i < KPAGE_BATCH / 64; i++)
		buf[i] = ~0ULL;

	for (long pfn = start_pfn2; pfn < end_pfn; pfn += KPAGE_BATCH) {
		int n = min((long)KPAGE_BATCH, end_pfn2 - pfn);
		buf[0] = buf[n / 64 - 1] = ~0ULL;
		if (pfn < start_pfn)
			buf[0] &= ~((1ULL << (start_pfn & 63)) - 1);
		if (pfn + n > end_pfn)
			buf[n / 64 - 1] &= (1ULL << (end_pfn & 63)) - 1;
		kpf_write(f, n / 64, KPAGEIDLE_PATH, buf);
	}
}

// Counts idle pages in range [start_pfn, end_pfn).
// Returns map: cg ino -> (idle anon, idle file).
static unordered_map<long, pair<long, long>>
count_idle_pages_per_cgroup(long start_pfn, long end_pfn) throw(error)
{
	// kpageidle requires pfn to be aligned by 64
	long start_pfn2 = start_pfn & ~63UL;
	long end_pfn2 = (end_pfn + 63) & ~63UL;

	fstream f_flags, f_cg, f_idle;
	kpf_open(KPAGEFLAGS_PATH, ios::in, start_pfn2, f_flags);
	kpf_open(KPAGECGROUP_PATH, ios::in, start_pfn2, f_cg);
	kpf_open(KPAGEIDLE_PATH, ios::in, start_pfn2 / 64, f_idle);

	uint64_t buf_flags[KPAGE_BATCH],
		 buf_cg[KPAGE_BATCH],
		 buf_idle[KPAGE_BATCH / 64];

	bool head_idle = false, head_anon = false;
	long head_cg = 0;
	int buf_index = KPAGE_BATCH;

	unordered_map<long, pair<long, long>> result;

	for (long pfn = start_pfn2; pfn < end_pfn; ++pfn, ++buf_index) {
		if (buf_index >= KPAGE_BATCH) {
			// buffer is empty - refill
			int n = min((long)KPAGE_BATCH, end_pfn2 - pfn);
			kpf_read(f_flags, n, KPAGEFLAGS_PATH, buf_flags);
			kpf_read(f_cg, n, KPAGECGROUP_PATH, buf_cg);
			kpf_read(f_idle, n / 64, KPAGEIDLE_PATH, buf_idle);
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

		auto &cnt = result[head_cg];
		if (head_anon)
			cnt.first++;
		else
			cnt.second++;
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

static PyObject *py_count_idle_pages_per_cgroup(PyObject *self, PyObject *args)
{
	long start_pfn, end_pfn;
	if (!PyArg_ParseTuple(args, "ll", &start_pfn, &end_pfn))
		return NULL;

	unordered_map<long, pair<long, long>> result;
	try {
		result = count_idle_pages_per_cgroup(start_pfn, end_pfn);
	} py_catch_error();

	// map the result to a PyDict
	py_ref dict = PyDict_New();
	if (!dict)
		return PyErr_NoMemory();

	for (auto &kv : result) {
		py_ref key = PyInt_FromLong(kv.first);
		py_ref val1 = PyInt_FromLong(kv.second.first);
		py_ref val2 = PyInt_FromLong(kv.second.second);
		if (!key || !val1 || !val2)
			return PyErr_NoMemory();

		py_ref val = PyTuple_Pack(2,
				(PyObject *)val1, (PyObject *)val2);
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

static PyMethodDef kpageutil_funcs[] = {
	{
		"set_idle_pages",
		(PyCFunction)py_set_idle_pages,
		METH_VARARGS, NULL,
	},
	{
		"count_idle_pages_per_cgroup",
		(PyCFunction)py_count_idle_pages_per_cgroup,
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
