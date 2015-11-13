import os


def _parse_meminfo():
    meminfo = {}
    with open('/proc/meminfo', 'r') as f:
        for l in f.readlines():
            l = l.split()
            val = int(l[1])
            if len(l) == 3 and l[2] == 'kB':
                val *= 1024
            meminfo[l[0].rstrip(':')] = val
    return meminfo


def _lookup_memcg_mount():
    with open('/etc/mtab', 'r') as f:
        for l in f:
            m = l.split()
            if m[2] == 'cgroup' and 'memory' in m[3].split(','):
                return m[1]
    raise RuntimeError("Memory cgroup not mounted")

PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")
MEM_TOTAL = _parse_meminfo()["MemTotal"]
MEMCG_MOUNT = _lookup_memcg_mount()
