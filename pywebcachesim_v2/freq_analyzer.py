import sys
import threading

from pymongo import MongoClient
from collections import defaultdict, Counter, deque

import os


class TraceIterator:
    def __init__(self, file_path):
        self.file_path = file_path
        self.i = 1

    def __iter__(self):
        """

        :return: generator((int, int, int))
        """
        file = open(self.file_path, 'r')
        next_line = file.readline()
        while next_line:
            timestamp, key, size = self.parse_line(next_line)
            yield self.i, timestamp, key, size
            self.i += 1
            next_line = file.readline()
        file.close()

    def head(self, n=1):
        lines = []
        file = open(self.file_path, 'r')
        for i in range(n):
            next_line = file.readline()
            lines.append(self.parse_line(next_line))
        file.close()
        return lines

    def parse_line(self, tr_data_line):
        # trace: (timestamp, key, size)
        split_line = tr_data_line.split(" ")
        if len(split_line) < 3:
            print(tr_data_line)
            raise ValueError
        try:
            timestamp = int(split_line[0])
        except:
            timestamp = split_line[0]
        try:
            size = int(split_line[2])
        except:
            raise
        try:
            key = int(split_line[1])
        except:
            key = split_line[1]
        return timestamp, key, size


class FreqBinnedWindowReqCount:
    def __init__(self, window_size, n_warmup):
        self.sliding_window = deque()
        self.counter = Counter()
        self.freq_bins = [0, 0, 0]
        self.window_size = window_size
        self.n_warmup = n_warmup

    def incr_count(self, index, key, size):
        if len(self.sliding_window) > self.window_size:
            key = self.sliding_window.popleft()
            self.counter[key] -= 1
        self.sliding_window.append(key)
        self.counter[key] += 1
        index = min(self.counter[key] - 1, len(self.freq_bins) - 1)

        if index > self.n_warmup:
            self.freq_bins[index] += size

    def result(self):
        return {
            "type": "BinnedwindowReqCount",
            "window_size": self.window_size,
            "freq_bins": self.freq_bins
        }


class FreqBinnedWindowObjCount:
    def __init__(self, window_size, n_warmup):
        self.freq_count_bins = [0, 0, 0]
        self.freq_byte_bins = [0, 0, 0]
        self.window_size = window_size
        self.window = deque()
        self.key_size_map = dict()
        self.n_warmup = n_warmup

    def dump_result(self):
        counter = Counter(self.window)
        for k, v in counter.items():
            i = min(v - 1, len(self.freq_count_bins) - 1)
            self.freq_count_bins[i] += v
            self.freq_byte_bins[i] += self.key_size_map[k]

    def incr_count(self, index, key, size):
        if len(self.window) == self.window_size:
            if index > self.n_warmup:
                self.dump_result()
            self.window = deque()
            self.key_size_map = dict()

        self.window.append(key)
        self.key_size_map[key] = size

    def result(self):
        return {
            "type": "BinnedWindowObjCount",
            "window_size": self.window_size,
            "freq_count_bins": self.freq_count_bins,
            "freq_byte_bins": self.freq_byte_bins
        }


class TraceStatistics:
    def __init__(self, n_warmup=-1):
        window_sizes = [100, 1000, 10000, 100000, 1000000, 10000000, 100000000]
        self.req_counters = []
        self.obj_counters = []
        for window_size in window_sizes:
            self.req_counters.append(FreqBinnedWindowReqCount(window_size, n_warmup))
            self.obj_counters.append(FreqBinnedWindowObjCount(window_size, n_warmup))

    def update(self, index, timestamp, key, size):
        for counter in self.req_counters:
            counter.incr_count(index, key, size)
        for counter in self.obj_counters:
            counter.incr_count(index, key, size)

    def get_result(self):
        for counter in self.obj_counters:
            counter.dump_result()

        return {
            "request_count_data": [counter.result() for counter in self.req_counters],
            "obj_count_data": [counter.result() for counter in self.obj_counters],
        }


WARMUP_MAP = {
    'wiki2018.tr': 2400 * 10 ** 6,
    'wiki2019.tr': 2200 * 10 ** 6,
    'traceUS_ts.tr': 200 * 10 ** 6,
    'traceHK_ts.tr': 200 * 10 ** 6,
    'ntg1_base.tr': 1000e6,
    'ntg2_base.tr': 1000e6,
    'ntg3_base.tr': 1000e6,
}


def analyze(trace_filepath):
    trace_iterator = TraceIterator(trace_filepath)
    trace_filename = trace_filepath.split("/")[-1]
    n_warmup = WARMUP_MAP[trace_filename]
    trace_statistics = TraceStatistics(n_warmup)
    for trace in trace_iterator:
        index, timestamp, key, size = trace
        if index != 0 and index % 1000000 == 0:
            print(f"[analyze] {index}")
        trace_statistics.update(index, timestamp, key, size)
    result = trace_statistics.get_result()
    result["trace_file"] = trace_filepath.split("/")[-1]
    return result


def analyze_and_write_to_mongo(trace_filename, dburi):
    trace_dir = os.environ["WEBCACHESIM_TRACE_DIR"]
    trace_filepath = f'{trace_dir}/{trace_filename}'
    print(f"[analyze_and_write_to_mongo] filepath: {trace_filepath}")
    print(f"[analyze_and_write_to_mongo] dburi: {dburi}")
    result = analyze(trace_filepath)
    print(result)
    client = MongoClient(dburi)
    db = client["webcachesim"]
    collection = db["freq_analysis"]
    collection.insert_one(result)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--trace_filename', type=str, nargs='?', required=True
    )
    parser.add_argument(
        '--dburi', type=str, nargs='?', required=True
    )
    args = parser.parse_args()
    analyze_and_write_to_mongo(
        args.trace_filename,
        args.dburi
    )
