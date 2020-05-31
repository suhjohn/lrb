import argparse
import uuid

def parse_cmd_args():
    # how to schedule parallel simulations
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug',
                        help='debug mode only run 1 task locally',
                        type=bool,
                        choices=[True, False])
    parser.add_argument('--authentication_file', type=str, nargs='?', help='authentication for database', required=True)
    parser.add_argument('--job_file', type=str, nargs='?', help='job config file', required=True)
    parser.add_argument('--algorithm_param_file', type=str, help='algorithm parameter config file', required=True)
    parser.add_argument('--trace_param_file', type=str, help='trace parameter config file', required=True)
    parser.add_argument('--pywebcachesim_task_id', type=str, help='task_id for the current batch execution', required=False,
                        default=str(uuid.uuid4().hex))
    args = parser.parse_args()
    return vars(args)

