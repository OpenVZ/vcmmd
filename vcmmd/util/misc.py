def roundup(v, t):
    return v if (v % t) == 0 else v + t - (v % t)
