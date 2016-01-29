from __future__ import absolute_import

from vcmmd.ve import Config as VEConfig

PATH = '/LoadManager'
BUS_NAME = 'com.virtuozzo.vcmmd'
IFACE = 'com.virtuozzo.vcmmd.LoadManager'


def ve_config_from_kv_array(kv_array):
    '''Convert an array of key-value tuples where key is an index of a config
    parameter in the VEConfig struct to key-value dictionary in which key is
    the name of the corresponding struct entry. Used to convert the input from
    dbus to the form accepted by the LoadManager class.
    '''
    dict_ = {}
    for k, v in kv_array:
        try:
            field_name = VEConfig._fields[k]
        except IndexError:
            # Silently ignore unknown fields in case the config is extended in
            # future
            continue
        dict_[field_name] = int(v)
    return dict_


def ve_config_to_kv_array(dict_):
    '''Convert a config dictionary to an array of pairs. The first value of
    each pair is the index of a config parameter in VEConfig struct while the
    second value is the value of the config parameter. Used to prepare a config
    for passing to dbus.
    '''
    kv_array = []
    for k in range(len(VEConfig._fields)):
        field_name = VEConfig._fields[k]
        try:
            kv_array.append((k, dict_[field_name]))
        except KeyError:
            pass  # No value is OK.
    return kv_array


def ve_config_from_array(arr):
    return ve_config_from_kv_array((i, arr[i])
                                   for i in range(len(arr)))
