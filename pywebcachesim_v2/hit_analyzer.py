import sys
import json

class TraceIterator:
    def __init__(self, file_path):
        self.i = 0
        self.key = 0
        self.size = 0
        self.file_path = file_path
        self.file = open(file_path)
        self.header = json.loads(self.file.readline())
        self.is_finished = False

    def __iter__(self):
        self.n = 0
        return self

    def __next__(self):
        if self.is_finished:
            raise StopIteration
        try:
            i, key, size = self.file.readline().split(" ")
        except:
            self.is_finished = True
            raise StopIteration
        # print(f"{self.file_path} {i} {key} {size}")
        return int(i), int(key), int(size)

class TraceDifference:
    def __init__(self, trace_iterator_a, trace_iterator_b):
        self.trace_iterator_a = trace_iterator_a
        self.trace_iterator_b = trace_iterator_b
        self.a_unique_hit_bytes = 0
        self.b_unique_hit_bytes = 0

        a_index, a_key, a_size = -1, -1, -1
        b_index, b_key, b_size = -1, -1, -1
        while self.trace_iterator_a.is_finished is False and self.trace_iterator_b.is_finished is False:
            try:
                if a_index == b_index:
                    a_index, a_key, a_size = self.trace_iterator_a.__next__()
                    b_index, b_key, b_size = self.trace_iterator_b.__next__()
                elif a_index < b_index:
                    # print(f"{trace_iterator_a.file_path} += {a_size}")
                    self.a_unique_hit_bytes += a_size
                    a_index, a_key, a_size = self.trace_iterator_a.__next__()
                else:
                    # print(f"{trace_iterator_b.file_path} += {b_size}")
                    self.b_unique_hit_bytes += b_size
                    b_index, b_key, b_size = self.trace_iterator_b.__next__()
            except:
                continue

        if self.trace_iterator_a.is_finished is False:
            while self.trace_iterator_a.is_finished is False:
                try:
                    if a_index == b_index:
                        a_index, a_key, a_size = self.trace_iterator_a.__next__()
                    else:
                        # print(f"{trace_iterator_a.file_path} += {b_size}")
                        self.a_unique_hit_bytes += a_size
                        a_index, a_key, a_size = self.trace_iterator_a.__next__()
                except:
                    continue

        else:
            while self.trace_iterator_b.is_finished is False:
                try:
                    if a_index == b_index:
                        b_index, b_key, b_size = self.trace_iterator_b.__next__()
                    else:
                        # print(f"{trace_iterator_b.file_path} += {b_size}")
                        self.b_unique_hit_bytes += b_size
                        b_index, b_key, b_size = self.trace_iterator_b.__next__()
                except:
                    continue


filepath_1 = sys.argv[1]
filepath_2 = sys.argv[2]
trace_iterator_a = TraceIterator(filepath_1)
trace_iterator_b = TraceIterator(filepath_2)
td = TraceDifference(trace_iterator_a, trace_iterator_b)
with open(f"{filepath_1}_X_{filepath_2}.json", "w") as f:
    json.dump({
        "f1_unique_hit_bytes": td.a_unique_hit_bytes,
        "f2_unique_hit_bytes": td.b_unique_hit_bytes,
        "f1": trace_iterator_a.header,
        "f2": trace_iterator_b.header
    }, f)
