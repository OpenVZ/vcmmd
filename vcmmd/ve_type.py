VE_TYPE_CT = 0
VE_TYPE_VM = 1
VE_TYPE_VM_LINUX = 2
VE_TYPE_VM_WINDOWS = 3

_TYPE_NAME = {
    VE_TYPE_CT: 'CT',
    VE_TYPE_VM: 'VM',
    VE_TYPE_VM_LINUX: 'VM_LIN',
    VE_TYPE_VM_WINDOWS: 'VM_WIN',
}

_NAME_TYPE = {v: k for k, v in _TYPE_NAME.iteritems()}


def get_ve_type_name(t):
    return _TYPE_NAME[t]


def lookup_ve_type_by_name(s):
    return _NAME_TYPE[s]


def get_all_ve_type_names():
    return sorted(_NAME_TYPE.keys())
