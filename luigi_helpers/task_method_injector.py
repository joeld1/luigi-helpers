import functools
import inspect
from copy import deepcopy
from typing import Dict, List

import luigi
from luigi.util import inherits

from luigi_helpers.misc_inspection_methods import rgetattr, rsetattr
from luigi_helpers.type_converters import get_unfrozen_dicts, cast_str_to_type


class TaskMethodInjector(object):

    def __init__(self, task_methods_to_wrap: Dict[str, List[str]] = None,
                 collect_args_kwargs: Dict[str, List[str]] = None):
        self.task_methods_to_wrap = task_methods_to_wrap
        self.collect_args_kwargs = collect_args_kwargs

    def __call__(self, foo, *args, **kwargs):
        self.wrap_and_inject_args_kwargs_into_clients_found_in_task_method(foo)
        self.collect_args_kwargs_from_client(foo)

        @functools.wraps(foo)
        def class_constructor(*args, **kwargs):
            return foo(*args, **kwargs)

        return class_constructor

    def collect_args_kwargs_from_client(self, foo):
        if self.collect_args_kwargs:
            for task_methods_to_wrap, luigi_params_containing_obj_to_collect_from in self.collect_args_kwargs.items():
                try:
                    old_method = getattr(foo, task_methods_to_wrap, None)
                    assert old_method
                except Exception as e:
                    print(e)
                    continue
                else:
                    luigi_method = TaskMethodInjector.arg_collector_luigi_method_wrapper(old_method,
                                                                                         luigi_params_containing_obj_to_collect_from)

                    setattr(foo, task_methods_to_wrap, luigi_method)  # Set the run method to the wrapper
                    print(f"Decorated {task_methods_to_wrap}!")

    def wrap_and_inject_args_kwargs_into_clients_found_in_task_method(self, foo):
        if self.task_methods_to_wrap:
            for task_methods_to_wrap, luigi_params_containing_injections in self.task_methods_to_wrap.items():
                try:
                    assert len(luigi_params_containing_injections) != 0
                    old_method = getattr(foo, task_methods_to_wrap, None)
                    assert old_method
                except Exception as e:
                    print(e)
                    continue
                else:
                    temp_method = TaskMethodInjector.arg_kwarg_injector_wrapper(old_method,
                                                                                luigi_params_containing_injections,
                                                                                task_methods_to_wrap)
                    setattr(foo, task_methods_to_wrap, temp_method)
                    print(f"Decorated {task_methods_to_wrap}!")

    @staticmethod
    def inject_params_into_luigi_task_instance_attr(self_obj, luigi_param_name: str = "method_arg_kwargs_to_inject",
                                                    task_attr_path: str = ""):
        current_params = self_obj.param_kwargs.copy()
        arg_kwargs_to_inject = current_params.get(luigi_param_name)
        methods_to_wrap = list(arg_kwargs_to_inject.keys())

        TaskMethodInjector.inject_params_into_client_method(self_obj, task_attr_path, methods_to_wrap,
                                                            arg_kwargs_to_inject)
        return self_obj

    @staticmethod
    def inject_params_into_client_method(self_obj, task_attr_path, methods_to_wrap, arg_kwargs_to_inject):
        # TODO: Refactor
        current_params = self_obj.param_kwargs.copy()
        for method_name in methods_to_wrap:
            attr_path_to_method = f"{task_attr_path}.{method_name}"
            try:
                prev_method = rgetattr(self_obj, attr_path_to_method)
            except AttributeError as e:
                print(e)
                continue

            method_injections_frzn = arg_kwargs_to_inject.get(method_name)
            flatten_params_frzn = current_params.get("flatten_params")
            unflatten_params_frzn = current_params.get("unflatten_params")

            @functools.wraps(prev_method)
            def temp_wrapped_method(*args2, **kwargs2):
                current_method_name = method_name
                current_attr_path = attr_path_to_method
                params_to_update = get_unfrozen_dicts(method_injections_frzn)
                flatten_params = dict(get_unfrozen_dicts(flatten_params_frzn)[0])
                flatten_params = {k: v for k, v in flatten_params.items() if not (v is None)}
                cast_str_to_type(flatten_params, "enumerate_types")

                unflatten_params = get_unfrozen_dicts(unflatten_params_frzn)[0]
                unflatten_params = {k: v for k, v in unflatten_params.items() if not (v is None)}

                for p in params_to_update:
                    name = p.get("name")
                    new_val = p.get("value")
                    is_flattened_key = p.get("is_flattened_key")
                    injection_type = p.get("injection_type")
                    param_location = p.get("param_location")

                    if not is_flattened_key:
                        print("Skipping indexing nested dicts")
                        if injection_type == "replace":
                            kwargs2[name] = new_val
                            print(f"Injected a new value for {name} into {current_attr_path}.{current_method_name}!")
                        else:
                            old_val = kwargs2.get(name, None)
                            if (not old_val is None) or new_val:
                                if isinstance(old_val, tuple) and isinstance(new_val, tuple):
                                    new_val = old_val + new_val
                                    kwargs2[name] = new_val
                                elif isinstance(old_val, dict) and isinstance(new_val, dict):
                                    prev_dict = deepcopy(old_val)
                                    prev_dict.update(new_val)
                                    kwargs2[name] = prev_dict
                                elif isinstance(old_val, list) and isinstance(new_val, list):
                                    prev_list = deepcopy(old_val)
                                    prev_list.extend(new_val)
                                    kwargs2[name] = prev_list
                                else:  # replace it
                                    kwargs2[name] = new_val
                                print(f"Updated {name} for {current_attr_path}.{current_method_name}!")
                    else:
                        # TODO: Merge nested dicts
                        print("lookup nested indices")
                fun = prev_method(*args2, **kwargs2)
                return fun

            rsetattr(self_obj, attr_path_to_method, temp_wrapped_method)

    @staticmethod
    def arg_collector_luigi_method_wrapper(old_method, luigi_params_containing_obj_to_collect_from):
        @functools.wraps(old_method)
        def luigi_method(self_obj, *args2, **kwargs2):  # Run/Requires
            TaskMethodInjector.modify_client_methods(self_obj, luigi_params_containing_obj_to_collect_from)

            fun = old_method(self_obj, *args2, **kwargs2)
            return fun

        return luigi_method

    @staticmethod
    def modify_client_methods(self_obj, luigi_params_containing_obj_to_collect_from):
        for attr_path_to_collect_from in luigi_params_containing_obj_to_collect_from:
            luigi_param_name = rgetattr(self_obj, attr_path_to_collect_from,
                                        None)  # luigi parameter containing list to collect
            if luigi_param_name:
                for attrs_to_get in luigi_param_name.copy():  # iterate through list to collect args kwargs for
                    callables_on_obj = TaskMethodInjector.get_callables_from_client_obj(self_obj, attrs_to_get)
                    for cur_method in callables_on_obj.copy():
                        attr_path_to_method = f"{attrs_to_get}.{cur_method}"

                        nested_method_wrapper = TaskMethodInjector.collect_args_kwargs_nested_client_method(self_obj,
                                                                                                            attr_path_to_method,
                                                                                                            cur_method)
                        prev_nested_method = rgetattr(self_obj, attr_path_to_method)
                        cur_signature = inspect.signature(prev_nested_method)
                        cur_signature = inspect.signature(nested_method_wrapper)
                        rsetattr(self_obj, attr_path_to_method, nested_method_wrapper)

    @staticmethod
    def collect_args_kwargs_nested_client_method(self_obj, attr_path_to_method, cur_method):
        prev_nested_method = rgetattr(self_obj, attr_path_to_method)

        @functools.wraps(prev_nested_method)
        def nested_method(*args3, **kwargs3):  # nested method found on client
            print("*" * 50)
            print(f"self.{attr_path_to_method} args and kwargs are:")
            print(args3)
            print(kwargs3)
            print("*" * 50)
            cur_signature = inspect.signature(prev_nested_method)
            self_obj.arg_kwargs_collected[attr_path_to_method].append(deepcopy(args3))
            self_obj.arg_kwargs_collected[attr_path_to_method].append(deepcopy(kwargs3))
            try:
                fun3 = prev_nested_method(*args3, **kwargs3)
            except Exception as e:
                print(e)
            # print("Wrapped nested function")
            return fun3

        return nested_method

    @staticmethod
    def get_callables_from_client_obj(self_obj, attrs_to_get):
        client_obj = rgetattr(self_obj, attrs_to_get, None)  # _client
        methods_found_on_obj = [m[0] for m in
                                inspect.getmembers(client_obj, predicate=inspect.ismethod)
                                if not (m[0].startswith('_'))].copy()
        callables_on_obj = [m for m in methods_found_on_obj if
                            callable(getattr(client_obj, m, None))].copy()
        return callables_on_obj

    @staticmethod
    def arg_kwarg_injector_wrapper(old_method, luigi_params_containing_injections, task_methods_to_wrap):
        @functools.wraps(old_method)
        def tmp_luigi_method(self_obj, *args2, **kwargs2):
            TaskMethodInjector.inject_args_kwargs_into_client_methods_wrapper(self_obj,
                                                                              luigi_params_containing_injections,
                                                                              task_methods_to_wrap)
            cur_signature = inspect.signature(old_method)
            fun = old_method(self_obj, *args2, **kwargs2)
            return fun

        cur_signature = inspect.signature(old_method)
        return tmp_luigi_method

    @staticmethod
    def inject_args_kwargs_into_client_methods_wrapper(self_obj, luigi_params_containing_injections,
                                                       task_methods_to_wrap):
        for luigi_config_injection_map in luigi_params_containing_injections:
            luigi_param_exists = self_obj.param_kwargs.get(luigi_config_injection_map, None)
            if luigi_param_exists:
                config_dict = luigi_param_exists.get_wrapped()
                for task_attr_containing_method_to_inject_into, task_param_names_to_lookup in config_dict.items():
                    for task_param_name in task_param_names_to_lookup:
                        TaskMethodInjector.inject_params_into_luigi_task_instance_attr(self_obj,
                                                                                       luigi_param_name=task_param_name,
                                                                                       task_attr_path=task_attr_containing_method_to_inject_into)
                        print(
                            f"Successfully decorated self.{task_methods_to_wrap}\n in order to modified self.{task_attr_containing_method_to_inject_into} by injecting dict found in the {self_obj.__class__.__name__}.{task_param_name}")


class FlattenParams(luigi.Task):
    reducer = luigi.Parameter(default='dot')
    inverse = luigi.BoolParameter(default=False)
    enumerate_types = luigi.ListParameter(default=['list', 'tuple'])
    keep_empty_types = luigi.ListParameter(default=None)


class UnflattenParams(luigi.Task):
    splitter = luigi.Parameter(default='tuple')
    inverse = luigi.BoolParameter(default=False)


@inherits(FlattenParams, UnflattenParams)
class FlattenDictParams(luigi.Task):
    flatten_params = luigi.DictParameter(default=FlattenParams().param_kwargs)
    unflatten_params = luigi.DictParameter(default=UnflattenParams().param_kwargs)