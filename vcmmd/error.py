VCMMD_ERROR_SUCCESS = 0
VCMMD_ERROR_INVALID_VE_NAME = 1
VCMMD_ERROR_INVALID_VE_TYPE = 2
VCMMD_ERROR_INVALID_VE_CONFIG = 3
VCMMD_ERROR_VE_NAME_ALREADY_IN_USE = 4
VCMMD_ERROR_VE_NOT_REGISTERED = 5
VCMMD_ERROR_VE_ALREADY_ACTIVE = 6
VCMMD_ERROR_VE_OPERATION_FAILED = 7
VCMMD_ERROR_UNABLE_APPLY_VE_GUARANTEE = 8
VCMMD_ERROR_VE_NOT_ACTIVE = 9


_ERRSTR = {
    0: 'Success',
    1: 'Invalid VE name',
    2: 'Invalid VE type',
    3: 'Invalid VE configuration',
    4: 'VE name already in use',
    5: 'VE not registered',
    6: 'VE already active',
    7: 'VE operation failed',
    8: 'Unable to apply VE guarantee',
    9: 'VE not active',
}


class VCMMDError(Exception):

    def __init__(self, errno):
        self.errno = errno

    def __str__(self):
        return _ERRSTR.get(self.errno, 'Unknown error')
