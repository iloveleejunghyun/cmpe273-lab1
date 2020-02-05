import io
import os
import numpy as np
import tempfile
import heapq
from itertools import count, groupby, islice

################################################################################  methods for ext merge sort ##########################################################################

def split_every(size, iterable):
    """Group stream into batches
    """
    c = count()
    for _, g in groupby(iterable, lambda x: next(c)//size):
        yield " ".join(map(str,list(g))) # or yield g if you want to output a generator


def group_every(size, iterable):
    """Group stream into batches
    """
    c = count()
    for _, g in groupby(iterable, lambda x: next(c)//size):
        l = list(g)
        yield len(l), map(str,l) # or yield g if you want to output a generator


def merge_files(mergers, output_file):
    int_streams = (map(int, f) for f in mergers)
    int_stream = heapq.merge(*int_streams)
    line_stream = map('{}\n'.format, int_stream)
    output_file.writelines(line_stream)


def reopen(fh, mode="r"):
    name = fh.name
    fh.close()
    return open(name, mode)


def close_unlink(fh):
    name = fh.name
    fh.close()
    os.unlink(name)


def merge_all_files(nums_mergers, max_files, last_mergers_list_fh, out_file_name):
    max_files = nums_mergers if nums_mergers < max_files else max_files
    while nums_mergers > 1:
        last_mergers_list_fh = reopen(last_mergers_list_fh)
        fnames_stream = (f.strip() for f in last_mergers_list_fh)
        mergers = tempfile.NamedTemporaryFile(delete=False, mode="w")
        for l, ten_files in group_every(max_files, fnames_stream):
            nums_mergers -= l
            with tempfile.NamedTemporaryFile(delete=False, mode="w") as merge:
                merge_files(map(open, ten_files), merge)
                mergers.write(merge.name+"\n")
                nums_mergers += 1
        close_unlink(last_mergers_list_fh)
        last_mergers_list_fh = mergers
    
    last_file = [line.strip() for line in reopen(last_mergers_list_fh).readlines()]
    assert len(last_file) == 1
  
    close_unlink(last_mergers_list_fh)

    with open(last_file[0], "r") as f:
        int_stream = (int(line) for line in f)
        line_stream = map('{}\n'.format, int_stream)
        with open(out_file_name, "w") as output_file:
            output_file.writelines(line_stream)

    os.unlink(last_file[0])
