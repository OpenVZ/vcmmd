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

MEM_TOTAL = _parse_meminfo()["MemTotal"]
