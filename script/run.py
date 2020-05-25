import json
import subprocess
import os
from multiprocessing.pool import ThreadPool

import yaml


def run():
    print("Running run.py")
    webcachesim_root = os.environ.get("WEBCACHESIM_ROOT")
    if not webcachesim_root:
        raise Exception(
            "WEBCACHESIM_ROOT is not set in the environment. WEBCACHESIM_ROOT is required for simulation execution.")
    dburi = os.environ.get("DBURI")
    if not dburi:
        raise Exception(
            "DBURI is not set in the environment. URI for MongoDB Atlas is required for simulation execution.")
    if not os.environ.get("WEBCACHESIM_TRACE_DIR"):
        raise Exception("WEBCACHESIM_TRACE_DIR is not set in the environment.")

    job_file = f'{webcachesim_root}/config/job_dev.yaml'
    algorithm_param_file = f'{webcachesim_root}/config/algorithm_params.yaml'
    trace_param_file = f'{webcachesim_root}/config/trace_params.yaml'
    execution_settings_file = f'{webcachesim_root}/config/execution_settings.yaml'

    if not os.path.exists(job_file):
        raise Exception(f"job file does not exist in the designated config path: {job_file}")
    if not os.path.exists(algorithm_param_file):
        raise Exception(
            f"algorithm parameter file does not exist in the designated config path: {algorithm_param_file}")
    if not os.path.exists(trace_param_file):
        raise Exception(f"trace parameter file does not exist in the designated config path: {trace_param_file}")
    if not os.path.exists(execution_settings_file):
        raise Exception(
            f"execution settings file does not exist in the designated config path: {execution_settings_file}")

    # HACK: We want to call setup.sh only when new nodes are added to the execution setting.
    #       We do this by keeping a temp list of hostnames of the previous execution and finding their diff
    with open(execution_settings_file) as f:
        execution_settings = yaml.load(f, Loader=yaml.FullLoader)
    nodes = execution_settings["nodes"]
    # nodes are in the following format: 1/ssh -p 22 ssuh@clnode035.clemson.cloudlab.us
    # we want to extract 'ssuh@clnode035.clemson.cloudlab.us'
    hostnames = []
    for node in nodes:
        node_ssh_cmd = node.split("/")[-1]
        node_name = node_ssh_cmd.split(" ")[-1]
        hostnames.append(node_name)
    try:
        with open(f'{webcachesim_root}/script/temp_hostnames.json', 'r') as f:
            prev_hostnames = json.load(f)
        diff_host_names = list(set(hostnames) - set(prev_hostnames))
    except FileNotFoundError: # case when invoked for the first time on the NFS node
        diff_host_names = list(set(hostnames)) # removing duplcates in case

    def _setup(hostname):
        command = f"ssh -o StrictHostKeyChecking=no {hostname} 'cd lrb; ./setup.sh'"
        proc = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True)
        proc.wait()
    pool = ThreadPool(30)
    pool.map(_setup, diff_host_names)
    pool.close()
    pool.join()

    with open(f'{webcachesim_root}/script/temp_hostnames.json', 'w') as f:
        json.dump(hostnames, f)

    command = f"python3 {webcachesim_root}/pywebcachesim/simulate.py --dburi {dburi} --job_file {job_file} --algorithm_param_file {algorithm_param_file} " \
              f"--trace_param_file {trace_param_file} --execution_settings_file {execution_settings_file} > ~/simulate_script.log"
    subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True)


if __name__ == "__main__":
    run()
