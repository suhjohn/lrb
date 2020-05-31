import argparse
import uuid
import yaml


def parse_cmd_args():
    # how to schedule parallel simulations
    parser = argparse.ArgumentParser()
    parser.add_argument('--dburi', type=str, nargs='?', help='mongodb database', required=True)
    parser.add_argument('--job_file', type=str, nargs='?', help='job config file', required=True)
    parser.add_argument('--algorithm_param_file', type=str, help='algorithm parameter config file', required=True)
    parser.add_argument('--trace_param_file', type=str, help='trace parameter config file', required=True)
    parser.add_argument('--execution_settings_file', type=str, required=True)
    parser.add_argument('--pywebcachesim_task_id', type=str, help='task_id for the current batch execution',
                        required=False,
                        default=str(uuid.uuid4().hex))
    args = parser.parse_args()
    args = vars(args)
    with open(args['job_file']) as f:
        job_params = yaml.load(f, Loader=yaml.FullLoader)
    with open(args['algorithm_param_file']) as f:
        default_algorithm_params = yaml.load(f, Loader=yaml.FullLoader)
    with open(args['trace_param_file']) as f:
        trace_params = yaml.load(f, Loader=yaml.FullLoader)
    with open(args['execution_settings_file']) as f:
        execution_settings = yaml.load(f, Loader=yaml.FullLoader)

    return args["pywebcachesim_task_id"], args["dburi"], \
           job_params, default_algorithm_params, trace_params, execution_settings
