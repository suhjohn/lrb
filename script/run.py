import subprocess
import os

webcachesim_root = os.environ.get("WEBCACHESIM_ROOT")
if not webcachesim_root:
    raise Exception(
        "WEBCACHESIM_ROOT is not set in the environment. WEBCACHESIM_ROOTis required for simulation execution.")
dburi = os.environ.get("DBURI")
if not dburi:
    raise Exception("DBURI is not set in the environment. URI for MongoDB Atlas is required for simulation execution.")
if not os.environ.get("WEBCACHESIM_TRACE_DIR"):
    raise Exception("WEBCACHESIM_TRACE_DIR is not set in the environment.")

job_file = f'{webcachesim_root}/config/job_dev.yaml'
algorithm_param_file = f'{webcachesim_root}/config/algorithm_params.yaml'
trace_param_file = f'{webcachesim_root}/config/trace_params.yaml'

if not os.path.exists(job_file):
    raise Exception(f"job file does not exist in the designated config path: {job_file}")
if not os.path.exists(algorithm_param_file):
    raise Exception(f"algorithm parameter file does not exist in the designated config path: {algorithm_param_file}")
if not os.path.exists(trace_param_file):
    raise Exception(f"trace parameter file does not exist in the designated config path: {trace_param_file}")

command = ['python3',
           f'{webcachesim_root}/pywebcachesim/simulate.py',
           '--dburi', dburi,
           '--job_file', job_file,
           '--algorithm_param_file', algorithm_param_file,
           '--trace_param_file', trace_param_file,
           ]

subprocess.run(command)
