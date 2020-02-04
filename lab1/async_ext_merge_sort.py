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
from queue import Queue
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, FIRST_COMPLETED

# Dear Professor Aung,
# With practice of asyncio, I find that asyncio can only create a single thread to run all async methods, which can not reduce the time to calculate, but also takes more time to solve
# the problem because task switching also need some time to realize. Considering that, I use threadpool instead of asyncio to realize the same function, which could be faster.
# Sincerely
# Yuan Wan



################################################################################  common varialbles ##########################################################################

#configure path
root_dir = os.path.dirname(os.path.abspath('.'))
input_dir = root_dir + '/lab1/input/'
output_dir = root_dir + '/lab1/output/'

data_queue = Queue()

MATRIX_SIZE = 50000
CHUNK_SIZE = 10 #num rows in chunk
MAX_FILES = 100 #maximum number of files for merging at the same time

################################################################################  common functions ##########################################################################

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

############################################################### external merge sort with asyncio #######################################################################

#read single file
async def read_single_file(file_name: str):
    input_file = open(file_name, "r")
    if input_file.mode == 'r':
        while True:
            num = input_file.readline()
            if not num: #end of the file
                break
            num = int(num.strip())
            data_queue.put(num)
    else:
        logger.error(f"Can't read the file:{fileName}! Please check.")
    input_file.close()

    pass


#read-data controller
def read_all_data(input_files, loop):
    asyncio.set_event_loop(loop)
    future = asyncio.gather(*(read_single_file(file_name) for file_name in input_files))
    loop.run_until_complete(future)
    global read_finished
    read_finished = True
    print("read-data finished")


def devide_data_to_temp_files(nums_per_temp_file, loop):
    asyncio.set_event_loop(loop)

    
    temp_data_list = []
    block = True
    while True:
        global read_finished
        if read_finished:
            if data_queue.empty():
                if len(temp_data_list) != 0:
                    asyncio.run(write_to_temp_file(temp_data_list))#!需要用线程池
                global devision_finished
                devision_finished = True
                print("devide-data finished")
                break

        data = data_queue.get(10000)
        temp_data_list.append(data)
        
        if len(temp_data_list) >= nums_per_temp_file:
            asyncio.run(write_to_temp_file(temp_data_list))
            temp_data_list = [] #!需要用线程池

async def write_to_temp_file(temp_data_list):
    with tempfile.NamedTemporaryFile(delete=False, mode="w") as merge:
        #sort part temp data and save it to a temp file
        temp_data_list.sort()
        merge.writelines(map("{}\n".format, temp_data_list))
        global last_mergers_list_fh
        last_mergers_list_fh.write(merge.name+"\n")
        global nums_mergers
        nums_mergers += 1

def external_merge_multifiles2(input_files:[], output_file, max_rows, size, max_files=10, show_progress=False, nums_per_temp_file=100):
    global last_mergers_list_fh
    last_mergers_list_fh = tempfile.NamedTemporaryFile(delete=False, mode="w")
    global nums_mergers
    nums_mergers = 0

    # create the loop to devide data
    thread_loop = asyncio.new_event_loop() 
    t = threading.Thread(target=devide_data_to_temp_files, args=(nums_per_temp_file, thread_loop,))
    t.daemon = True
    t.start()

    #create the loop to read unsorted data
    thread_loop = asyncio.new_event_loop() 
    t = threading.Thread(target=read_all_data, args=(input_files, thread_loop,))
    t.daemon = True
    t.start()

    global devision_finished
    while True:
        if devision_finished:
            break

    total_rows, total_cols = size
    
    #finish devide

    max_files = nums_mergers if nums_mergers < max_files else max_files

    while nums_mergers > 1:
        last_mergers_list_fh = reopen(last_mergers_list_fh)
        fnames_stream = (f.strip() for f in last_mergers_list_fh)
        mergers = tempfile.NamedTemporaryFile(delete=False, mode="w")
        for l, ten_files in group_every(max_files, fnames_stream):
           # print(l)
           # print(list(ten_files))
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
        #print(list(int_stream))
        #print()
        line_stream = map('{}\n'.format, split_every(total_cols, int_stream))
        #print(list(line_stream))
        output_file.writelines(line_stream)

    os.unlink(last_file[0])

############################################################### external merge sort with thread pool #######################################################################
#read single file
def read_single_file_threadpool(file_name: str):
    input_file = open(file_name, "r")
    if input_file.mode == 'r':
        while True:
            num = input_file.readline()
            if not num: #end of the file
                break
            num = int(num.strip())
            data_queue.put(num)
    else:
        logger.error(f"Can't read the file:{fileName}! Please check.")
    input_file.close()

    pass


#read-data controller
def read_all_data_threadpool(input_files, loop):
    asyncio.set_event_loop(loop)

    executor = ThreadPoolExecutor(max_workers=4)
    all_task = [executor.submit(read_single_file_threadpool, (file_name)) for file_name in input_files]

    wait(all_task, return_when=ALL_COMPLETED)
    global read_finished
    read_finished = True
    print("read-data finished")


def devide_data_to_temp_files(nums_per_temp_file, loop):
    asyncio.set_event_loop(loop)
    executor = ThreadPoolExecutor(max_workers=8)
    global write_temp_file_tasks

    temp_data_list = []
    block = True
    while True:
        global read_finished
        if read_finished:
            if data_queue.empty():
                if len(temp_data_list) != 0:
                    write_temp_file_tasks.append(executor.submit(write_to_temp_file_threadpool, (temp_data_list)))
                global devision_finished
                devision_finished = True
                print("devide-data finished")
                
                break

        data = data_queue.get(10000)
        temp_data_list.append(data)
        
        if len(temp_data_list) >= nums_per_temp_file:
            write_temp_file_tasks.append(executor.submit(write_to_temp_file_threadpool, (temp_data_list)))
            temp_data_list = [] #!需要用线程池

def write_to_temp_file_threadpool(temp_data_list):
    with tempfile.NamedTemporaryFile(delete=False, mode="w") as merge:
        #sort part temp data and save it to a temp file
        temp_data_list.sort()
        merge.writelines(map("{}\n".format, temp_data_list))
        global last_mergers_list_fh
        last_mergers_list_fh.write(merge.name+"\n")
        global nums_mergers
        nums_mergers += 1

def external_merge_multi_files_with_threadpool(input_files:[], output_file, max_rows, size, max_files=10, show_progress=False, nums_per_temp_file=100):
    global last_mergers_list_fh
    last_mergers_list_fh = tempfile.NamedTemporaryFile(delete=False, mode="w")
    global nums_mergers
    nums_mergers = 0

    # create the loop to devide data
    thread_loop = asyncio.new_event_loop()
    t = threading.Thread(target=devide_data_to_temp_files, args=(nums_per_temp_file, thread_loop,))
    t.daemon = True
    t.start()

    #create the loop to read unsorted data
    thread_loop = asyncio.new_event_loop() 
    t = threading.Thread(target=read_all_data_threadpool, args=(input_files, thread_loop,))
    t.daemon = True
    t.start()

    global devision_finished
    while True:
        if devision_finished:
            break
    global write_temp_file_tasks
    wait(write_temp_file_tasks, return_when=ALL_COMPLETED)
    #finish devide

    total_rows, total_cols = size

    max_files = nums_mergers if nums_mergers < max_files else max_files

    while nums_mergers > 1:
        last_mergers_list_fh = reopen(last_mergers_list_fh)
        fnames_stream = (f.strip() for f in last_mergers_list_fh)
        mergers = tempfile.NamedTemporaryFile(delete=False, mode="w")
        for l, ten_files in group_every(max_files, fnames_stream):
           # print(l)
           # print(list(ten_files))
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
        #print(list(int_stream))
        #print()
        line_stream = map('{}\n'.format, split_every(total_cols, int_stream))
        #print(list(line_stream))
        output_file.writelines(line_stream)

    os.unlink(last_file[0])

if __name__ == "__main__":
    s = time.perf_counter()
    max_files = 50

    size = (MATRIX_SIZE, MATRIX_SIZE)
    out = "sorted.txt"
    
    outFileName = f"{output_dir}sorted.txt"
    output_file = open(outFileName, 'w')
    # for i in range(10):
    #     inFileName = f"{input_dir}unsorted_{str(i+1)}.txt"
    #     input_files.append(open(inFileName))
    # external_merge_multifiles(input_files, output_file, CHUNK_SIZE, size, max_files, True)

    devision_finished = False
    read_finished = False
    input_file_names = []
    for i in range(10):
        input_file_names.append(f"{input_dir}unsorted_{str(i+1)}.txt")
    # external_merge_multifiles2(input_file_names, output_file, CHUNK_SIZE, size, max_files, True)
    write_temp_file_tasks = []
    external_merge_multi_files_with_threadpool(input_file_names, output_file, CHUNK_SIZE, size, max_files, True)

    output_file.close()
    elapsed = time.perf_counter() - s
    print(f"{__file__} executed in {elapsed:0.4f} seconds.")
