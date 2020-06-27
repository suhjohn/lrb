from pymongo import MongoClient
from collections import defaultdict

import os


class AgeObjectBins:
    def __init__(self, max_pow):
        self.bins = [0 for _ in range(max_pow + 1)]
        self.last_accessed = defaultdict(int)  # key: last access logical time
        self.avg_interval = defaultdict(int)
        self.counter = defaultdict(int)
        self.max_bin_size = max_pow
        self.bin_list = []

    def record_stat(self):
        # self.bin_list.append(list(self.bins))
        # for i in range(len(self.bins)):
        #     self.bins[i] = 0
        for average in self.avg_interval.values():
            age_index = min(int(average).bit_length(), self.max_bin_size)
            self.bins[age_index] += 1
        self.bin_list = list(self.bins)

    def incr_count(self, curr_index, key):
        if self.last_accessed[key]:
            interreq_interval = curr_index - self.last_accessed[key]
            self.avg_interval[key] = ((self.avg_interval[key] * self.counter[key]) + interreq_interval) / \
                                     (self.counter[key] + 1)
        self.last_accessed[key] = curr_index

    def get_bins(self):
        return list(self.bin_list)


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


class TraceStatistics:
    def __init__(self):
        self.age_obj_bins = AgeObjectBins(35)
        self.segment_window = 1000000

    def record_stat(self):
        self.age_obj_bins.record_stat()

    def update(self, index, timestamp, key, size):
        self.age_obj_bins.incr_count(index, key)

    def get_result(self):
        return {
            "interreq_obj_count_bins": self.age_obj_bins.get_bins(),
        }


def analyze(trace_filepath):
    trace_statistics = TraceStatistics()
    trace_iterator = TraceIterator(trace_filepath)
    for trace in trace_iterator:
        index, timestamp, key, size = trace
        trace_statistics.update(index, timestamp, key, size)
    trace_statistics.record_stat()
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
    collection = db["obj_interreq_data"]
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
