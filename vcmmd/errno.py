SUCCESS = 0
INVALID_VE_NAME = 1
INVALID_VE_TYPE = 2
INVALID_VE_CONFIG = 3
VE_NAME_ALREADY_IN_USE = 4
VE_NOT_REGISTERED = 5
VE_ALREADY_ACTIVE = 6
VE_OPERATION_FAILED = 7
NO_SPACE = 8
VE_NOT_ACTIVE = 9


_ERRSTR = {
    0: 'Success',
    1: 'Invalid VE name',
    2: 'Invalid VE type',
    3: 'Invalid VE configuration',
    4: 'VE name already in use',
    5: 'VE not registered',
    6: 'VE already active',
    7: 'VE operation failed',
    8: 'No space for VE',
    9: 'VE not active',
}


def strerror(err):
    return _ERRSTR.get(err, 'Unknown error')
