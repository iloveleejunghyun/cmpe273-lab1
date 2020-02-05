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


data_queue = Queue()

read_finished = False
devision_finished = False
write_temp_file_tasks = []

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

############################################################### external merge sort with asyncio #######################################################################

#read single file
async def read_single_file_asyncio(file_name: str):
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


#read-data controller
def read_all_data_asyncio(input_files, loop):
    asyncio.set_event_loop(loop)
    future = asyncio.gather(*(read_single_file_asyncio(file_name) for file_name in input_files))
    loop.run_until_complete(future)
    global read_finished
    read_finished = True
    print("read-data finished")


def devide_data_to_temp_files_asyncio(nums_per_temp_file, loop):
    asyncio.set_event_loop(loop)

    
    temp_data_list = []
    block = True
    while True:
        global read_finished
        if read_finished:
            if data_queue.empty():
                if len(temp_data_list) != 0:
                    asyncio.run(write_to_temp_file_asyncio(temp_data_list))#!需要用线程池
                global devision_finished
                devision_finished = True
                print("devide-data finished")
                break

        data = data_queue.get()
        temp_data_list.append(data)
        
        if len(temp_data_list) >= nums_per_temp_file:
            asyncio.run(write_to_temp_file_asyncio(temp_data_list))
            temp_data_list = []

async def write_to_temp_file_asyncio(temp_data_list):
    with tempfile.NamedTemporaryFile(delete=False, mode="w") as merge:
        #sort part temp data and save it to a temp file
        temp_data_list.sort()
        merge.writelines(map("{}\n".format, temp_data_list))
        global last_mergers_list_fh
        last_mergers_list_fh.write(merge.name+"\n")
        global nums_mergers
        nums_mergers += 1

def external_merge_multi_files_with_asyncio(input_file_names:[], out_file_name, max_files=10, show_progress=False, nums_per_temp_file=100):
    global last_mergers_list_fh
    last_mergers_list_fh = tempfile.NamedTemporaryFile(delete=False, mode="w")
    global nums_mergers
    nums_mergers = 0

    # create the loop to devide data
    thread_loop = asyncio.new_event_loop() 
    t = threading.Thread(target=devide_data_to_temp_files_asyncio, args=(nums_per_temp_file, thread_loop,))
    t.daemon = True
    t.start()

    #create the loop to read unsorted data
    thread_loop = asyncio.new_event_loop() 
    t = threading.Thread(target=read_all_data_asyncio, args=(input_file_names, thread_loop,))
    t.daemon = True
    t.start()

    global devision_finished
    while True:
        if devision_finished:
            break
    
    #merge
    merge_all_files(nums_mergers, max_files, last_mergers_list_fh, out_file_name)


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
    # print(f"read finish in {file_name}")
    pass


#read-data controller
def read_all_data_threadpool(input_files):
    # asyncio.set_event_loop(loop)

    executor = ThreadPoolExecutor(max_workers=4)
    all_task = [executor.submit(read_single_file_threadpool, (file_name)) for file_name in input_files]

    wait(all_task, return_when=ALL_COMPLETED)
    global read_finished
    read_finished = True
    print("read-data finished")


def devide_data_to_temp_files_threadpool(nums_per_temp_file, loop):
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

        
        if data_queue.empty() == False:
            data = data_queue.get(False)
            temp_data_list.append(data)
        
            if len(temp_data_list) >= nums_per_temp_file:
                write_temp_file_tasks.append(executor.submit(write_to_temp_file_threadpool, (temp_data_list)))
                temp_data_list = []
        else:
            time.sleep(0.0001) # It's not a good method, but I don't konw how to yield to other thread now.

            

def write_to_temp_file_threadpool(temp_data_list):
    with tempfile.NamedTemporaryFile(delete=False, mode="w") as merge:
        #sort part temp data and save it to a temp file
        temp_data_list.sort()
        merge.writelines(map("{}\n".format, temp_data_list))
        global last_mergers_list_fh
        last_mergers_list_fh.write(merge.name+"\n")
        global nums_mergers
        nums_mergers += 1

def external_merge_multi_files_with_threadpool(input_file_names:[], out_file_name, max_files=10, show_progress=False, nums_per_temp_file=100):
    global last_mergers_list_fh
    last_mergers_list_fh = tempfile.NamedTemporaryFile(delete=False, mode="w")
    global nums_mergers
    nums_mergers = 0

    # create the loop to devide data
    thread_loop = asyncio.new_event_loop()
    t = threading.Thread(target=devide_data_to_temp_files_threadpool, args=(nums_per_temp_file, thread_loop,))
    t.daemon = True
    t.start()

    read_all_data_threadpool(input_file_names)

    global devision_finished
    while True:
        if devision_finished:
            break
    global write_temp_file_tasks
    wait(write_temp_file_tasks, return_when=ALL_COMPLETED)
    #finish devide

    merge_all_files(nums_mergers, max_files, last_mergers_list_fh, out_file_name)
   

######################################################################### main() to test #############################################################################

if __name__ == "__main__":

    root_dir = os.path.dirname(os.path.abspath('.'))
    input_dir = root_dir + '/lab1/input/'
    output_dir = root_dir + '/lab1/output/'

    max_files = 50

    out = "sorted.txt"
    out_file_name = f"{output_dir}sorted.txt"

    input_file_names = []
    for i in range(10):
        input_file_names.append(f"{input_dir}unsorted_{str(i+1)}.txt")

    #test and time ext merge sort method with asyncio 
    time_start = time.perf_counter()
    external_merge_multi_files_with_asyncio(input_file_names, out_file_name, max_files, True)
    elapsed_asyncio = time.perf_counter() - time_start

    #test and time ext merge sort method with threadpool 
    time_start = time.perf_counter()
    devision_finished = False
    read_finished = False
    external_merge_multi_files_with_threadpool(input_file_names, out_file_name, max_files, True)
    elapsed_threadpool = time.perf_counter() - time_start

    #log times to output/async_time.file
    print(f"{__file__} executed in asyncio={elapsed_asyncio:0.4f} and threadpool={elapsed_threadpool:0.4f} seconds.")
    with open(f"{output_dir}async_time.txt", mode="w") as time_file:
        time_file.write(f"asyncio consumed time={elapsed_asyncio:0.4f} seconds\n")
        time_file.write(f"threadpool consumed time={elapsed_threadpool:0.4f} seconds\n")
