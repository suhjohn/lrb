import re
import time
import subprocess
import json
import random
from multiprocessing.pool import ThreadPool


def get_running_tasks(hostname):
    command = f"ssh {hostname} 'ps aux | grep webcachesim'"
    res_str = subprocess.check_output(command, shell=True, stderr=None, timeout=10).decode("utf-8")
    return list(set([val for val in re.findall(r'(?<=task_id=)[a-zA-Z0-9_]+', res_str)]))


def run_task(args):
    hostname, task = args
    command = f"ssh {hostname} '{task} & disown'"
    subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True)


########################

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


def run(pywebcachesim_task_id, raw_tasks, execution_settings):
    # debug mode, only 1 task
    log_file = f"/tmp/{pywebcachesim_task_id}--logfile.json"

    tasks = list()
    for i, task in enumerate(raw_tasks):
        task_id, task_str = to_task_str(task)
        task_str = f'{task_str} > /tmp/{task_id}.log\n'
        tasks.append((task_id, task_str))

    print(f"task_count: {len(tasks)}")
    nodes = [node.split("/")[1] for node in execution_settings['nodes']]
    max_node_available_cores = {
        node.split("/")[1]: int(node.split("/")[0]) for node in execution_settings['nodes']
    }
    total_task_count = len(tasks)
    completed_task_count = 0
    running_task_ids = {node: [] for node in nodes}
    node_statistics = {node: {} for node in nodes}
    with open(log_file, "w") as f:
        res = {
            "completed_task_count": completed_task_count,
            "total_task_count": total_task_count,
            "node_statistics": node_statistics
        }
        json.dump(res, f)
    while completed_task_count < total_task_count:
        # determine what tasks were completed and are still running
        try:
            pool = ThreadPool(5)
            nodes_webcachesim_task_ids = pool.map(get_running_tasks, nodes)
            pool.close()
            pool.join()
        except:
            print("Failed to get running tasks from all nodes. Sleeping 20")
            time.sleep(30)
            continue
        # we only care about task_ids assigned for this runner call. There may exist other task_ids invoked by
        #   another webcachesim calls; we simply ignore them.
        queried_task_ids = {
            node: list(set(node_webcachesim_task_ids).intersection(set(running_task_ids[node])))
            for node, node_webcachesim_task_ids in zip(nodes, nodes_webcachesim_task_ids)
        }
        # remove task_ids that are completed as shown from the queried task ids: if they're done, then they would
        #   not be in the queried task_ids
        completed_task_ids = {
            node: list(set(task_ids) - set(queried_task_ids[node]))
            for node, task_ids in running_task_ids.items()
        }
        running_task_ids = {
            node: list(set(task_ids) - set(completed_task_ids[node]))
            for node, task_ids in running_task_ids.items()
        }
        for node, task_ids in completed_task_ids.items():
            completed_task_count += len(task_ids)
            node_statistics[node].setdefault("completed_task_count", 0)
            node_statistics[node]["completed_task_count"] += len(task_ids)

        with open(log_file, "w") as f:
            res = {
                "task_id": pywebcachesim_task_id,
                "completed_task_count": completed_task_count,
                "total_task_count": total_task_count,
                "node_statistics": node_statistics
            }
            json.dump(res, f)
        # assign nodes tasks
        #    just need to know how many available cores
        if tasks:
            available_nodes = []
            for node in nodes:
                available_nodes.extend([
                    node for _ in range(max_node_available_cores[node] - len(running_task_ids[node]))
                ])
            random.shuffle(available_nodes)
            new_tasks = []
            for node in available_nodes:
                if not tasks:
                    break
                task_id, task_str = tasks.pop()
                new_tasks.append((node, task_str))
                running_task_ids[node].append(task_id)
                print(node, task_str, task_id)

            # execute new tasks
            print(f"running {len(new_tasks)}")
            if new_tasks:
                pool = ThreadPool(5)
                pool.map(run_task, new_tasks)
                pool.close()
                pool.join()

        time.sleep(20)
