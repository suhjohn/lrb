from pymongo import MongoClient
from collections import defaultdict

import os


class AgeObjectBins:
    def __init__(self, max_pow):
        self.bins = [0 for _ in range(max_pow + 1)]
        self.last_accessed = defaultdict(int)  # key: last access logical time
        self.max_bin_size = max_pow
        self.bin_list = []

    def record_stat(self):
        self.bin_list.append(list(self.bins))
        for i in range(len(self.bins)):
            self.bins[i] = 0

    def incr_count(self, curr_index, key):
        if self.last_accessed[key]:
            age_index = min((curr_index - self.last_accessed[key]).bit_length(), self.max_bin_size)
            self.bins[age_index] += 1
        self.last_accessed[key] = curr_index

    def get_bins(self):
        return list(self.bin_list)


class AgeByteBins:
    def __init__(self, max_pow):
        self.bins = [0 for _ in range(max_pow + 1)]
        self.last_accessed = defaultdict(int)  # key: last access logical time
        self.max_bin_size = max_pow
        self.bin_list = []

    def record_stat(self):
        self.bin_list.append(list(self.bins))
        for i in range(len(self.bins)):
            self.bins[i] = 0

    def incr_count(self, curr_index, key, size):
        if self.last_accessed[key]:
            age_index = min((curr_index - self.last_accessed[key]).bit_length(), self.max_bin_size)
            self.bins[age_index] += size
        self.last_accessed[key] = curr_index

    def get_bins(self):
        return list(self.bin_list)


class FreqObjectBins:
    def __init__(self, max_pow):
        self.freq_counter = defaultdict(int)
        self.bins = [0 for _ in range(max_pow + 1)]
        self.max_bin_size = max_pow
        self.bin_list = []

    def record_stat(self):
        self.bin_list.append(list(self.bins))
        for i in range(len(self.bins)):
            self.bins[i] = 0

    def incr_count(self, curr_index, key):
        self.freq_counter[key] += 1
        freq_index = min(self.freq_counter[key].bit_length(), self.max_bin_size)
        self.bins[freq_index] += 1

    def get_bins(self):
        return list(self.bin_list)


class FreqByteBins:
    def __init__(self, max_pow):
        self.freq_counter = defaultdict(int)
        self.bins = [0 for _ in range(max_pow + 1)]
        self.max_bin_size = max_pow
        self.bin_list = []

    def record_stat(self):
        self.bin_list.append(list(self.bins))
        for i in range(len(self.bins)):
            self.bins[i] = 0

    def incr_count(self, curr_index, key, size):
        self.freq_counter[key] += 1
        freq_index = min(self.freq_counter[key].bit_length(), self.max_bin_size)
        self.bins[freq_index] += size

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
        self.age_byte_bins = AgeByteBins(35)
        self.freq_obj_bins = FreqObjectBins(35)
        self.freq_byte_bins = FreqByteBins(35)
        self.segment_window = 1000000

    def record_stat(self):
        self.age_obj_bins.record_stat()
        self.age_byte_bins.record_stat()
        self.freq_obj_bins.record_stat()
        self.freq_byte_bins.record_stat()

    def update(self, index, timestamp, key, size):
        if index != 0 and index % self.segment_window == 0:
            self.record_stat()

        self.age_obj_bins.incr_count(index, key)
        self.age_byte_bins.incr_count(index, key, size)
        self.freq_obj_bins.incr_count(index, key)
        self.freq_byte_bins.incr_count(index, key, size)

    def get_result(self):
        return {
            "age_obj_bins": self.age_obj_bins.get_bins(),
            "age_byte_bins": self.age_byte_bins.get_bins(),
            "freq_obj_bins": self.freq_obj_bins.get_bins(),
            "freq_byte_bins": self.freq_byte_bins.get_bins()
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
    collection = db["trace_analysis"]
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
