class Singleton(type):
    '''Singleton metaclass.

    Usage:

    class MySingleton(object):
        __metaclass__ = Singleton
        ...
    '''

    _instances = {}

    def __call__(cls, *args, **kwargs):
        try:
            inst = cls._instances[cls]
        except KeyError:
            inst = super(Singleton, cls).__call__(*args, **kwargs)
            cls._instances[cls] = inst
        return inst
