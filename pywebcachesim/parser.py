import argparse


def parse_cmd_args():
    # how to schedule parallel simulations
    parser = argparse.ArgumentParser()
    parser.add_argument('--dburi', type=str, nargs='?', help='mongodb database', required=True)
    parser.add_argument('--job_file', type=str, nargs='?', help='job config file', required=True)
    parser.add_argument('--algorithm_param_file', type=str, help='algorithm parameter config file', required=True)
    parser.add_argument('--trace_param_file', type=str, help='trace parameter config file', required=True)
    parser.add_argument('--execution_settings_file', type=str, required=True)
    args = parser.parse_args()

    return vars(args)

