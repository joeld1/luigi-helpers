from luigi.freezing import FrozenOrderedDict


def get_unfrozen_dicts(frzn_dicts):
    """
    Recursively unfreeze nested ordered dictionaries
    :param frzn_dicts:
    :return:
    """
    if isinstance(frzn_dicts, tuple):
        unfrozen_dicts_ls = []
        for m in frzn_dicts:
            new_dict = get_unfrozen_dicts(m)[0]
            unfrozen_dicts_ls.append(new_dict)
    elif isinstance(frzn_dicts, FrozenOrderedDict):
        unfrozen_dicts = frzn_dicts.get_wrapped()
        new_unfrozen_dict = {}
        for k, v in unfrozen_dicts.items():
            if isinstance(v, FrozenOrderedDict):
                next_dicts = get_unfrozen_dicts(v)[0]
            elif isinstance(v, (list, tuple)):
                next_dicts = [get_unfrozen_dicts(v2)[0] for v2 in v]
            else:
                next_dicts = v
            new_unfrozen_dict[k] = next_dicts
        unfrozen_dicts_ls = [new_unfrozen_dict]
    else:
        return (frzn_dicts,)
    return unfrozen_dicts_ls


def cast_str_to_type(flatten_params, key_to_types):
    list_of_types = flatten_params.get(key_to_types, None)
    all_types = []
    if list_of_types:
        all_types = [getattr(globals().get("__builtins__"), k, None) for k in list_of_types]
        all_types = filter(None, all_types)
    flatten_params[key_to_types] = tuple(all_types)