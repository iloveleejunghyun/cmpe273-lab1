import io
import os
import sys
import time

import tempfile

from queue import Queue
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, FIRST_COMPLETED

import utils

# Dear Professor Aung,
# With practice of asyncio, I find that asyncio can only create a single thread to run all async methods, which can not reduce the time to calculate, but also takes more time to solve
# the problem because task switching also need some time to realize. Considering that, I use threadpool instead of asyncio to realize the same function, which could be faster.
# Sincerely
# Yuan Wan



################################################################################  common varialbles ##########################################################################

root_dir = os.path.dirname(os.path.abspath('.'))
input_dir = root_dir + '/lab1/input/'
output_dir = root_dir + '/lab1/output/'

#temprarily save the readed data
data_queue = Queue()

#used to lock the main thread until devision-process finished
write_temp_file_tasks = []

read_finished = False
devision_finished = False
nums_mergers = 0


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


def sort_asyncio(input_file_names:[], out_file_name, max_files=10, show_progress=False, nums_per_temp_file=100):
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
    utils.merge_all_files(nums_mergers, max_files, last_mergers_list_fh, out_file_name)


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


#read-data controller
def read_all_data_threadpool(input_files):

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
            time.sleep(0.0001) # It's not a good method, but I don't know how to yield to other thread now.


def write_to_temp_file_threadpool(temp_data_list):
    with tempfile.NamedTemporaryFile(delete=False, mode="w") as merge:
        #sort part temp data and save it to a temp file
        temp_data_list.sort()
        merge.writelines(map("{}\n".format, temp_data_list))
        global last_mergers_list_fh
        last_mergers_list_fh.write(merge.name+"\n")
        global nums_mergers
        nums_mergers += 1


def sort_threadpool(input_file_names:[], out_file_name, max_files=10, show_progress=False, nums_per_temp_file=100):
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

    utils.merge_all_files(nums_mergers, max_files, last_mergers_list_fh, out_file_name)
   

######################################################################### main() to test #############################################################################


if __name__ == "__main__":

    max_files = 50

    out = "sorted.txt"
    out_file_name = f"{output_dir}sorted.txt"

    input_file_names = []
    for i in range(10):
        input_file_names.append(f"{input_dir}unsorted_{str(i+1)}.txt")

    #test and time ext merge sort method with asyncio 
    time_start = time.perf_counter()
    sort_asyncio(input_file_names, out_file_name, max_files, True)
    elapsed_asyncio = time.perf_counter() - time_start

    #test and time ext merge sort method with threadpool 
    time_start = time.perf_counter()
    devision_finished = False
    read_finished = False
    sort_threadpool(input_file_names, out_file_name, max_files, True)
    elapsed_threadpool = time.perf_counter() - time_start

    #log times to output/async_time.file
    result = f"sorting with asyncio consumed {elapsed_asyncio:0.4f} seconds.\n"
    result += f"sorting with threadpool consumed {elapsed_threadpool:0.4f} seconds.\n"
    result += f"the speed with threadpool is {(elapsed_asyncio/elapsed_threadpool):0.2f} times compared to that of asyncio."
    print(result)

    with open(f"{output_dir}async_time.txt", mode="w") as time_file:
        time_file.write(result)
