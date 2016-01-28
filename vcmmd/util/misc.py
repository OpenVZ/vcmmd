def roundup(v, t):
    return v if (v % t) == 0 else v + t - (v % t)

def clamp(v, l, h):
    return max(l, min(v, h))
