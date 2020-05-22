import yaml
from pygit2 import Repository
import subprocess
import os

webcachesim_root = '/proj/cops-PG0/workspaces/ssuh/lrb'
dburi = os.environ.get("DB_URI")
if not dburi:
    raise Exception("DB_URI is not set in the environment. MongoDB URI is required for simulation execution.")

command = ['python3',
           f'{webcachesim_root}/pywebcachesim/simulate.py',
           '--dburi', dburi,
           '--job_file', f'{webcachesim_root}/config/job_dev.yaml',
           '--algorithm_param_file', f'{webcachesim_root}/config/algorithm_params.yaml',
           '--trace_param_file', f'{webcachesim_root}/config/trace_params.yaml',
           ]

subprocess.run(command)
