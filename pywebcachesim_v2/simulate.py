import sys
from pywebcachesim_v2 import parser, get_task, runner


def simulate():
    args = parser.parse_cmd_args()
    # f string is supported in 3.6
    if sys.version_info[0] < 3 or (sys.version_info[0] == 3 and sys.version_info[1] < 6):
        raise Exception('Error: python version need to be at least 3.6')

    pywebcachesim_task_id = args.pop("pywebcachesim_task_id")
    tasks = get_task.get_task(args)
    if args["debug"]:
        print(tasks)
    return runner.run(args, tasks, pywebcachesim_task_id)


if __name__ == '__main__':
    simulate()
