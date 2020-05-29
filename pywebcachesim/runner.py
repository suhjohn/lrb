import time
import subprocess
import uuid

def to_task_str(task: dict):
    """
    split deterministic args and nodeterminstics args. Add _ prefix to later
    """

    params = {}
    for k, v in task.items():
        if k not in ['trace_file', 'cache_type', 'cache_size'] and v is not None:
            params[k] = str(v)
    task_id = str(int(time.time() * 1000000))
    # use timestamp as task id
    params['task_id'] = task_id
    params = [f'--{k}={v}' for k, v in params.items()]
    params = ' '.join(params)
    res = f'webcachesim_cli {task["trace_file"]} {task["cache_type"]} {task["cache_size"]} {params}'
    return task_id, res


def run(execution_settings: dict, tasks: list):
    ts = str(uuid.uuid4().hex)
    job_file = f"/tmp/{ts}.job"
    log_file = f"/tmp/{ts}.log"
    with open(log_file, 'w') as f:
        f.write(f'n_task:{len(tasks)}\njob_file:{job_file}')
    with open(job_file, 'w') as f:
        for i, task in enumerate(tasks):
            task_id, task_str = to_task_str(task)
            task_str = f'bash --login -c "{task_str}" &> /tmp/{task_id}.log\n'
            f.write(task_str)

    with open(log_file, "w") as log_file_p:
        with open(job_file) as f:
            command = ['parallel', '-v', '--eta', '--shuf', '--sshdelay', '0.1']
            for n in execution_settings['nodes']:
                command.extend(['-S', n])
            subprocess.run(command,
                           stdout=log_file_p,
                           stderr=log_file_p,
                           stdin=f)
