from vcmmd import Error
from vcmmd import errno as _errno
from vcmmd.ve import types as ve_types
from vcmmd.ve.ct import CT
from vcmmd.ve.vm import VM


_VE_CLASS_LIST = [CT, VM]


def _lookup_ve_class(ve_type):
    for ve_class in _VE_CLASS_LIST:
        if ve_class.VE_TYPE == ve_type:
            return ve_class


def _ve_name_ok(ve_name):
    assert isinstance(ve_name, basestring)
    if not ve_name:
        return False
    if '/' in ve_name:
        return False
    return True


def make(ve_name, ve_type):
    if not _ve_name_ok(ve_name):
        raise Error(_errno.INVALID_VE_NAME)
    ve_class = _lookup_ve_class(ve_type)
    if not ve_class:
        raise Error(_errno.INVALID_VE_TYPE)
    return ve_class(ve_name)
