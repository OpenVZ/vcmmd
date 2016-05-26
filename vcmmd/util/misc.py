def roundup(v, t):
    return v if (v % t) == 0 else v + t - (v % t)


def clamp(v, l, h):
    return max(l, min(v, h))


def sorted_by_val(d):
    return sorted(d, key=lambda k: d[k])
