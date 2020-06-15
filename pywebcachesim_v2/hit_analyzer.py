import sys
import json

filepath_1 = sys.argv[1]
filepath_2 = sys.argv[2]

f1 = open(filepath_1)
f2 = open(filepath_2)
f1_header = json.loads(f1.readline())
f2_header = json.loads(f2.readline())
f1_unique_hit_bytes = 0
f2_unique_hit_bytes = 0

incr_f1 = True
incr_f2 = True
f1_complete = False
f2_complete = False
i = 0
early_stop = -1
while True:
    if i == early_stop:
        f1_complete = True
        f2_complete = True
        break
    if incr_f1:
        try:
            f1_index, f1_key, f1_size = f1.readline().split(" ")
            f1_index, f1_key, f1_size = int(f1_index), int(f1_key), int(f1_size)
        except:
            f1_complete = True
            break
    if incr_f2:
        try:
            f2_index, f2_key, f2_size = f2.readline().split(" ")
            f2_index, f2_key, f2_size = int(f2_index), int(f2_key), int(f2_size)
        except:
            f2_complete = True
            break

    # print(f"{i} ({f1_index}, {f1_key}, {f1_size}), ({f2_index}, {f2_key}, {f2_size})")
    if f1_index < f2_index:
        incr_f1 = True
        incr_f2 = False
        # print(f"    increment for f1: {f1_key}")
        f1_unique_hit_bytes += f1_size
    elif f1_index > f2_index:
        incr_f1 = False
        incr_f2 = True
        # print(f"    increment for f2: {f2_key}")
        f2_unique_hit_bytes += f2_size
    else:
        incr_f1 = True
        incr_f2 = True
        if f1_key != f2_key:
            # print(f"increment for {f1_key} {f2_key}")
            f1_unique_hit_bytes += f1_size
            f2_unique_hit_bytes += f2_size

    i += 1

if not f1_complete:
    while True:
        try:
            f1_index, f1_key, f1_size = f1.readline().split(" ")
            f1_index, f1_key, f1_size = int(f1_index), int(f1_key), int(f1_size)
        except:
            break
        if f1_index != f2_index:
            f1_unique_hit_bytes += f1_size
        else:
            if f1_key != f2_key:
                f1_unique_hit_bytes += f1_size
                f2_unique_hit_bytes += f2_size
elif not f2_complete:
    while True:
        try:
            f2_index, f2_key, f2_size = f2.readline().split(" ")
            f2_index, f2_key, f2_size = int(f2_index), int(f2_key), int(f2_size)
        except:
            break
        if f1_index != f2_index:
            f1_unique_hit_bytes += f1_size
        else:
            if f1_key != f2_key:
                f1_unique_hit_bytes += f1_size
                f2_unique_hit_bytes += f2_size

f1.close()
f2.close()
with open(f"{filepath_1}_X_{filepath_2}.json", "w") as f:
    json.dump({
        "f1_unique_hit_bytes": f1_unique_hit_bytes,
        "f2_unique_hit_bytes": f2_unique_hit_bytes,
        "f1": f1_header,
        "f2": f2_header
    }, f)
