import io
import os
import sys
import time

import tempfile

import utils

################################################################################ varialbles definition ##########################################################################

#configure path
root_dir = os.path.dirname(os.path.abspath('.'))
input_dir = root_dir + '/lab1/input/'
output_dir = root_dir + '/lab1/output/'


############################################################################# synchronous methods to sort #############################################################################

def read_and_devide(input_file_names, last_mergers_list_fh, nums_per_temp_file):
    cur_row = 0
    nums_mergers = 0
    input_file_index = 0
    input_files = []
    for file_name in input_file_names:
        input_files.append(open(file_name)) 
    input_file = input_files[input_file_index]
    temp_data_list = []
    while True:
        line = input_file.readline()
        if not line:
            input_file_index += 1
            if input_file_index < len(input_files):
                input_file = input_files[input_file_index]
                last_offset = 0
                continue
            else:
                break

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
    #finish devide
    for input_file in input_files:
        input_file.close()
    return nums_mergers

def sort_sync(input_file_names:[], out_file_name, max_files=10, show_progress=False, nums_per_temp_file=100):
    
    last_mergers_list_fh = tempfile.NamedTemporaryFile(delete=False, mode="w")

    nums_mergers = read_and_devide(input_file_names, last_mergers_list_fh, nums_per_temp_file)

    #merge
    utils.merge_all_files(nums_mergers, max_files, last_mergers_list_fh, out_file_name)


######################################################################### main() to test #############################################################################

if __name__ == "__main__":
    s = time.perf_counter()

    test_times = 100
    for i in range(test_times):

        max_files = 50
        out = "sorted.txt"
        out_file_name = f"{output_dir}sorted.txt"
        input_file_names = []
        for i in range(10):
            input_file_names.append(f"{input_dir}unsorted_{str(i+1)}.txt")

        sort_sync(input_file_names, out_file_name, max_files, True)

    elapsed = time.perf_counter() - s

    #log times to output/async_time.file
    result = f"Sync-sort costs {elapsed:0.4f} seconds for {test_times} times. Average time = {(elapsed/test_times):0.4f} seconds"
    print(result)
    with open(f"{output_dir}sync_time.txt", mode="w") as time_file:
        time_file.write(result)