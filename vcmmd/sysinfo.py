import os


def _get_end_pfn():
    end_pfn = 0
    with open('/proc/zoneinfo', 'r') as f:
        for l in f.readlines():
            l = l.split()
            if l[0] == 'spanned':
                end_pfn = int(l[1])
            elif l[0] == 'start_pfn:':
                end_pfn += int(l[1])
    return end_pfn


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

END_PFN = _get_end_pfn()
PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")
MEM_TOTAL = _parse_meminfo()["MemTotal"]
