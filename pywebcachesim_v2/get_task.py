import yaml


def cartesian_product(param: dict):
    worklist = [param]
    res = []

    while len(worklist):
        p = worklist.pop()
        split = False
        for k in p:
            if type(p[k]) == list:
                _p = {_k: _v for _k, _v in p.items() if _k != k}
                for v in p[k]:
                    worklist.append({
                        **_p,
                        k: v,
                    })
                split = True
                break

        if not split:
            res.append(p)

    return res


def get_task(dburi, job_params, algorithm_params, trace_params):
    """
    convert job config to list of task
    @:returns dict/[dict]
    """
    assert "trace_files" in job_params, "trace_files not set in file params."
    assert "cache_types" in job_params, "cache_types not set in file params."
    trace_files = job_params['trace_files']
    cache_types = job_params['cache_types']

    tasks = []
    for trace_file in trace_files:
        for cache_type in cache_types:
            for cache_size_or_size_parameters in trace_params[trace_file]['cache_sizes']:
                # element can be k: v or k: list[v], which would be expanded with cartesian product
                # priority: default < per trace < per trace per algorithm < per trace per algorithm per cache size
                parameters = {}
                if cache_type in algorithm_params:
                    parameters = {**parameters, **algorithm_params[cache_type]}
                per_trace_params = {}
                for k, v in trace_params[trace_file].items():
                    if k not in ['cache_sizes'] and k not in algorithm_params and v is not None:
                        per_trace_params[k] = v
                parameters = {**parameters, **per_trace_params}
                if cache_type in trace_params[trace_file]:
                    # trace parameters overwrite default parameters
                    parameters = {**parameters, **trace_params[trace_file][cache_type]}
                if isinstance(cache_size_or_size_parameters, dict):
                    # only 1 key (single cache size) is allowed
                    assert (len(cache_size_or_size_parameters) == 1)
                    cache_size = list(cache_size_or_size_parameters.keys())[0]
                    if cache_type in cache_size_or_size_parameters[cache_size]:
                        # per cache size parameters overwrite other parameters
                        parameters = {**parameters, **cache_size_or_size_parameters[cache_size][cache_type]}
                else:
                    cache_size = cache_size_or_size_parameters
                parameters_list = cartesian_product(parameters)
                for parameters in parameters_list:
                    task = {
                        'trace_file': trace_file,
                        'cache_type': cache_type,
                        'cache_size': cache_size,
                        'dburi': dburi,
                        **parameters,
                    }
                    for k, v in job_params.items():
                        if k not in [
                            'cache_types',
                            'trace_files',
                        ] and v is not None:
                            task[k] = v
                    tasks.append(task)
    return tasks
