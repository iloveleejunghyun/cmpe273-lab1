




Dear Professor Aung,
With practice of asyncio, I find that asyncio can only create a single thread to run all async methods, which can not reduce the time to calculate, 
but also takes more time to solve the problem because task switching also need some time to realize. Considering that, I use threadpool instead of asyncio 
to realize the same function, which could be faster.

Here's result for one time:
Sync-sort costs 2.3803 seconds for 100 times. Average time = 0.0238 seconds
Asyncio-sort costs 11.8859 seconds for 10 times. Average time=1.1886.
Threadpool-sort costs 1.2094 seconds for 10 times. Average time=0.1209.

The result shows that syncronous external merge method is faster than the other two asyncronous methods. I think the main reason is that the data set is too small and the program spends too much time on thread switching.

The result also shows that the asyncronoues method with threadpool is much faster than that of asyncio, which suggests my theory is right. 

Sincerely
Yuan Wan


committed file:

1. ext_merge_sort.py
2. async_ext_merge_sort.py
3. all unsorted_*.txt files
4. all sorted_.txt files
5. sync_time.txt and async_time.txt
6. utils.py



