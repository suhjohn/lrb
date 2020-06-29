import json
import subprocess
import os
import time
import uuid
import re
from multiprocessing.pool import ThreadPool
from subprocess import DEVNULL
from collections import deque

import yaml
from os import path

DBURI = os.environ['DBURI']
TELEGRAM_CHAT_ID = os.environ['TELEGRAM_CHAT_ID']
TELEGRAM_API_KEY = os.environ['TELEGRAM_API_KEY']
WEBCACHESIM_ROOT = os.environ['WEBCACHESIM_ROOT']
PYTHON = '/usr/bin/python3'


class WebcachesimExecutor:
    def __init__(self, config_dir, dburi, telegram_api_key, telegram_chat_id):
        self.config_dir = config_dir
        self.dburi = dburi
        self.telegram_chat_id = telegram_chat_id
        self.telegram_api_key = telegram_api_key

    def _send_telegram(self, msg):
        import telegram
        bot = telegram.Bot(token=self.telegram_api_key)
        size = 800
        print(msg)
        if len(msg) > size:
            messages = [msg[start: start + size] for start in range(0, len(msg) - 1, size)]
            for message in messages:
                bot.send_message(chat_id=self.telegram_chat_id, text=message)
                time.sleep(0.3)
        else:
            bot.send_message(chat_id=self.telegram_chat_id, text=msg)

    def _run_program(self, command, **kwargs):
        self._send_telegram(f'run: {command}')
        try:
            p = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, **kwargs)
            self._send_telegram(p.stdout.decode("utf-8"))
            return p.stdout.decode("utf-8")
        except Exception as e:
            self._send_telegram(f'{command} error: {e}')
            return str(e)

    def _setup_nodes(self, nodes):
        hostnames = []
        for node in nodes:
            node_ssh_cmd = node.split("/")[-1]
            node_name = node_ssh_cmd.split(" ")[-1]
            hostnames.append(node_name)

        try:
            with open(f'/tmp/temp_hostnames.json', 'r') as f:
                prev_hostnames = json.load(f)
            # diff_host_names = list(set(hostnames) - set(prev_hostnames))
            diff_host_names = list(set(hostnames))
        except FileNotFoundError:  # case when invoked for the first time on the NFS node
            diff_host_names = list(set(hostnames))  # removing duplcates in case

        def _setup(hostname):
            command = f"ssh -o StrictHostKeyChecking=no {hostname} 'cd {WEBCACHESIM_ROOT}; ./setup.sh'"
            print(command)
            p = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
            try:
                return p.stdout.decode()
            except:
                return p.stdout

        def _build(hostname):
            command = f"ssh -o StrictHostKeyChecking=no {hostname} 'cd {WEBCACHESIM_ROOT}; ./build.sh'"
            print(command)
            p = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
            return p.stdout.decode()

        pool = ThreadPool(5)
        pool.map(_setup, diff_host_names)
        pool.close()
        pool.join()

        pool = ThreadPool(5)
        pool.map(_build, diff_host_names)
        pool.close()
        pool.join()

        with open(f'/tmp/temp_hostnames.json', 'w') as f:
            json.dump(hostnames, f)

    def run(self):
        task_id = self._run_program("git log --pretty=format:'%h' -n 1")
        command = f'git submodule update --init --recursive'
        subprocess.run(command, cwd=WEBCACHESIM_ROOT, shell=True)

        command = f"nohup {PYTHON} {WEBCACHESIM_ROOT}/pywebcachesim_v2/simulate.py " \
                  f"--dburi {DBURI} " \
                  f"--algorithm_param_file {WEBCACHESIM_ROOT}/{self.config_dir}/algorithm_params.yaml " \
                  f"--execution_settings_file {WEBCACHESIM_ROOT}/{self.config_dir}/execution_settings.yaml " \
                  f"--job_file {WEBCACHESIM_ROOT}/{self.config_dir}/job_dev.yaml " \
                  f"--trace_param_file {WEBCACHESIM_ROOT}/{self.config_dir}/trace_params.yaml " \
                  f"--pywebcachesim_task_id {task_id} &> /tmp/{task_id}--execution.log & disown"
        self._send_telegram(f"[{task_id}] Start simulation")
        subprocess.Popen(command, executable="/bin/bash", shell=True)

        log_file = f"/tmp/{task_id}--logfile.json"
        for i in range(3):
            if not path.exists(log_file):
                time.sleep(10)
            else:
                break
        self._run_program("ps aux | grep python3")
        if not path.exists(log_file):
            self._send_telegram(f"No task execution found. Exceeded max wait.")

        with open(log_file, 'r') as f:
            data = json.load(f)
        self._send_telegram(json.dumps(data))
        self._send_telegram(f"[{task_id}] start")

        # Ping Telegram every 60 seconds
        total_task_count = data['total_task_count']
        curr_task_count, prev_task_count = 0, 0
        percent_executed = 0.05
        while curr_task_count < total_task_count:
            with open(log_file) as f:
                data = json.load(f)
                curr_task_count = data['completed_task_count']
                if curr_task_count - prev_task_count > total_task_count * percent_executed:
                    prev_task_count = curr_task_count
                    self._send_telegram(json.dumps(data))
            time.sleep(20)
        self._send_telegram(f"[{task_id}] complete")

    def setup(self):
        job_file = f"{WEBCACHESIM_ROOT}/{self.config_dir}/execution_settings.yaml"
        with open(job_file) as f:
            execution_settings = yaml.load(f, Loader=yaml.FullLoader)
        nodes = execution_settings["nodes"]
        self._setup_nodes(nodes)
        self._send_telegram(f"Setup complete")


    def _build_nodes(self, nodes):
        hostnames = []
        for node in nodes:
            node_ssh_cmd = node.split("/")[-1]
            node_name = node_ssh_cmd.split(" ")[-1]
            hostnames.append(node_name)

        try:
            with open(f'/tmp/temp_hostnames.json', 'r') as f:
                prev_hostnames = json.load(f)
            # diff_host_names = list(set(hostnames) - set(prev_hostnames))
            diff_host_names = list(set(hostnames))
        except FileNotFoundError:  # case when invoked for the first time on the NFS node
            diff_host_names = list(set(hostnames))  # removing duplcates in case

        def _build(hostname):
            command = f"ssh -o StrictHostKeyChecking=no {hostname} 'cd {WEBCACHESIM_ROOT}; ./build.sh'"
            p = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
            return p.stdout.decode()

        self._send_telegram(f"[build_nodes] start")
        pool = ThreadPool(5)
        messages = pool.map(_build, diff_host_names)
        pool.close()
        pool.join()

        self._send_telegram(f"[build_nodes] complete")
        with open(f'/tmp/temp_hostnames.json', 'w') as f:
            json.dump(hostnames, f)

    def build(self):
        job_file = f"{WEBCACHESIM_ROOT}/{self.config_dir}/execution_settings.yaml"
        with open(job_file) as f:
            execution_settings = yaml.load(f, Loader=yaml.FullLoader)
        nodes = execution_settings["nodes"]
        self._build_nodes(nodes)


if __name__ == '__main__':
    executor = WebcachesimExecutor("config", DBURI, TELEGRAM_API_KEY, TELEGRAM_CHAT_ID)
    # executor.build()
    # executor.run()
    executor._run_program("ps aux | grep python")