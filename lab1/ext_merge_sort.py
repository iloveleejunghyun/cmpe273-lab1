import io
import os
import sys
import numpy as np
import tempfile
import heapq
import time
import tqdm
from itertools import count, groupby, islice
import logging

# confiure log
logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger("areq")
logging.getLogger("chardet.charsetprober").disabled = True

#configure path
root_dir = os.path.dirname(os.path.abspath('.'))
input_dir = root_dir + '/lab1/input/'
output_dir = root_dir + '/lab1/output/'



MATRIX_SIZE = 50000
CHUNK_SIZE = 5 #num rows in chunk
MAX_FILES = 100 #maximum number of files for merging at the same time


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

def external_merge_multifiles2(input_files:[], output_file, max_rows, size, max_files=10, show_progress=False, nums_per_temp_file=100):
    """Main function uses external merge sort method
        -----------------
        parameters

        input_files: opened files handler with the matrix to sort
        output_file: opened file handler where the result will be saved
        max_rows: number of rows in a chunk
        size: size of the original matrix to sort
        max_files: number of files to be merged in the same moment
        show_progress: if True - show a progress bar, print log messages
    """
    total_rows, total_cols = size
    max_rows = total_rows if total_rows < max_rows else max_rows
    #split data, write to files
    cur_row = 0
    nums_mergers = 0
    last_mergers_list_fh = tempfile.NamedTemporaryFile(delete=False, mode="w")
    if show_progress:
        start_time = time.time()
        print("Spliting data into files")
        pbar = tqdm.tqdm(total=total_rows)
    input_file_index = 0
    input_file = input_files[input_file_index]
    temp_data_list = []
    while cur_row < total_rows:
        line = input_file.readline()
        if not line:
            input_file_index += 1
            if input_file_index < len(input_files):
                input_file = input_files[input_file_index]
                last_offset = 0
                continue
            else:
                break
        #print(line)
        chunk_data = int(line.strip())
        cur_row += 1
        temp_data_list.append(chunk_data)
        if len(temp_data_list) >= nums_per_temp_file:
            with tempfile.NamedTemporaryFile(delete=False, mode="w") as merge:
                #sort part temp data and save it to a temp file
                temp_data_list.sort()
                merge.writelines(map("{}\n".format, temp_data_list))
                temp_data_list.clear()
                last_mergers_list_fh.write(merge.name+"\n")
                nums_mergers += 1
            if show_progress:
                pbar.update(1)
    #finish devide

    print(f"total cur_row:{cur_row}")

    if show_progress:
        print("Done in {} seconds".format(time.time() - start_time))
        print("Start merging process")
        start_time = time.time()
        pbar.close()
        pbar = tqdm.tqdm(total=nums_mergers) #TODO: calc total using nums_mergers and max_files

    max_files = nums_mergers if nums_mergers < max_files else max_files

    while nums_mergers > 1:
        last_mergers_list_fh = reopen(last_mergers_list_fh)
        fnames_stream = (f.strip() for f in last_mergers_list_fh)
        mergers = tempfile.NamedTemporaryFile(delete=False, mode="w")
        for l, ten_files in group_every(max_files, fnames_stream):
           # print(l)
           # print(list(ten_files))
            nums_mergers -= l
            if show_progress:
                pbar.update(l)
            with tempfile.NamedTemporaryFile(delete=False, mode="w") as merge:
                merge_files(map(open, ten_files), merge)
                mergers.write(merge.name+"\n")
                nums_mergers += 1
        close_unlink(last_mergers_list_fh)
        last_mergers_list_fh = mergers
    
    last_file = [line.strip() for line in reopen(last_mergers_list_fh).readlines()]
    assert len(last_file) == 1

    close_unlink(last_mergers_list_fh)

    if show_progress:
        print("Done in {} seconds".format(time.time() - start_time))
        print("Start reshaping output file")
        start_time = time.time()
        pbar.close()

    with open(last_file[0], "r") as f:
        int_stream = (int(line) for line in f)
        #print(list(int_stream))
        #print()
        line_stream = map('{}\n'.format, split_every(total_cols, int_stream))
        #print(list(line_stream))
        output_file.writelines(line_stream)

    os.unlink(last_file[0])

    if show_progress:
        print("Done in {} seconds".format(time.time() - start_time))

def external_merge_multifiles(input_files:[], output_file, max_rows, size, max_files=10, show_progress=False):
    """Main function uses external merge sort method
        -----------------
        parameters

        input_files: opened files handler with the matrix to sort
        output_file: opened file handler where the result will be saved
        max_rows: number of rows in a chunk
        size: size of the original matrix to sort
        max_files: number of files to be merged in the same moment
        show_progress: if True - show a progress bar, print log messages
    """
    total_rows, total_cols = size
    max_rows = total_rows if total_rows < max_rows else max_rows
    #split data, write to files
    cur_row = 0
    last_offset = 0
    nums_mergers = 0
    last_mergers_list_fh = tempfile.NamedTemporaryFile(delete=False, mode="w")
    if show_progress:
        start_time = time.time()
        print("Spliting data into files")
        pbar = tqdm.tqdm(total=total_rows)
    input_file_index = 0
    input_file = input_files[input_file_index]
    while cur_row < total_rows:
        input_file.seek(last_offset)
        line = input_file.readline()
        if not line:
            input_file_index += 1
            if input_file_index < len(input_files):
                input_file = input_files[input_file_index]
                last_offset = 0
                continue
            else:
                break
        last_offset += len(line)
        #print(line)
        chunk_data = sorted(list(map(int, line.strip().split(" "))))
        cur_row += 1
        with tempfile.NamedTemporaryFile(delete=False, mode="w") as merge:
            merge.writelines(map('{}\n'.format, chunk_data))
            last_mergers_list_fh.write(merge.name+"\n")
            nums_mergers += 1
        if show_progress:
            pbar.update(1)
    #finish devide
    print(f"total cur_row:{cur_row}")

    if show_progress:
        print("Done in {} seconds".format(time.time() - start_time))
        print("Start merging process")
        start_time = time.time()
        pbar.close()
        pbar = tqdm.tqdm(total=nums_mergers) #TODO: calc total using nums_mergers and max_files

    max_files = nums_mergers if nums_mergers < max_files else max_files

    while nums_mergers > 1:
        last_mergers_list_fh = reopen(last_mergers_list_fh)
        fnames_stream = (f.strip() for f in last_mergers_list_fh)
        mergers = tempfile.NamedTemporaryFile(delete=False, mode="w")
        for l, ten_files in group_every(max_files, fnames_stream):
            nums_mergers -= l
            if show_progress:
                pbar.update(l)
            with tempfile.NamedTemporaryFile(delete=False, mode="w") as merge:
                merge_files(map(open, ten_files), merge)
                mergers.write(merge.name+"\n")
                nums_mergers += 1
        close_unlink(last_mergers_list_fh)
        last_mergers_list_fh = mergers
    
    last_file = [line.strip() for line in reopen(last_mergers_list_fh).readlines()]
    assert len(last_file) == 1

    close_unlink(last_mergers_list_fh)

    if show_progress:
        print("Done in {} seconds".format(time.time() - start_time))
        print("Start reshaping output file")
        start_time = time.time()
        pbar.close()

    with open(last_file[0], "r") as f:
        int_stream = (int(line) for line in f)
        #print(list(int_stream))
        #print()
        line_stream = map('{}\n'.format, split_every(total_cols, int_stream))
        #print(list(line_stream))
        output_file.writelines(line_stream)

    os.unlink(last_file[0])

    if show_progress:
        print("Done in {} seconds".format(time.time() - start_time))

def calc_chunk_mem():
    """Calculates actual size of readed chunk
    """
    size=(MATRIX_SIZE, CHUNK_SIZE)
    a = np.random.randint(np.iinfo(np.int32).min,high=np.iinfo(np.int32).max, size=size, dtype=np.int32)
    return sys.getsizeof(a) // (1024)

def calc_files_mem(max_files):
    """Calculates size of opened files (file handler + buffer size)
    """
    size = 0
    with tempfile.TemporaryFile() as fh:
        size = sys.getsizeof(fh) * max_files 
    size +=  io.DEFAULT_BUFFER_SIZE * max_files  
    return size // (1024)


if __name__ == "__main__":
    s = time.perf_counter()
    max_files = 50
    print("Chunk size {} kB".format(calc_chunk_mem()))
    print("Open files size {} kB".format(calc_files_mem(max_files)))
    size = (MATRIX_SIZE, MATRIX_SIZE)
    out = "sorted.txt"
    input_files = []
    for i in range(10):
        inFileName = f"{input_dir}unsorted_{str(i+1)}.txt"
        input_files.append(open(inFileName)) 
    outFileName = f"{output_dir}sorted.txt"
    output_file = open(outFileName, 'w')
    external_merge_multifiles2(input_files, output_file, CHUNK_SIZE, size, max_files, True)

    for input_file in input_files:
        input_file.close()
    output_file.close()
    elapsed = time.perf_counter() - s
    logger.info(f"{__file__} executed in {elapsed:0.4f} seconds.")
