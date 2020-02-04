#
from itertools import count, groupby, islice
import asyncio
import threading

def height_class(h):
    if h > 180:
        return "tall"
    elif h < 160:
        return "short"
    else:
        return "middle"

friends = [191, 158, 159, 165, 170, 177, 181, 182, 190]

friends = sorted(friends, key = height_class)
for m, n in groupby(friends, key = height_class):
    print(m)
    print(list(n))


    
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, FIRST_COMPLETED
import time

# 参数times用来模拟网络请求的时间
def get_html(times):
    time.sleep(times)
    print("get page {}s finished".format(times))
    return times

executor = ThreadPoolExecutor(max_workers=2)
urls = [3, 2, 4] # 并不是真的url
all_task = [executor.submit(get_html, (url)) for url in urls]
wait(all_task, return_when=ALL_COMPLETED)
print("main")

# def devide_data_to_temp_files(nums_per_temp_file, loop):

#     asyncio.set_event_loop(loop)

#     temp_data_list = []
#     while True:
#         temp_data_list.append(data_queue.get())
#         if len(temp_data) >= nums_per_temp_file:
#             asyncio.run(write_to_temp_file(temp_data_list))
#             temp_data_list = [] #new

# if __name__ == '__main__':
    
#     # 创建一个事件循环thread_loop
#     thread_loop = asyncio.new_event_loop() 

#     # 将thread_loop作为参数传递给子线程
#     t = threading.Thread(target=devide_data_to_temp_files, args=(10, thread_loop,))
#     t.daemon = True
#     t.start()

#     main_loop = asyncio.get_event_loop()


#     async def main_work():
#         while True:
#             print('main on loop:%s' % id(main_loop))
#             await asyncio.sleep(4)


#     main_loop.run_until_complete(main_work())

# 定义一个协程
# async def slow_operation(future):
#     await asyncio.sleep(1)
#     future.set_result('Future is done!')

# # 获得全局循环事件
# loop = asyncio.get_event_loop()
# # 实例化期物对象
# future = asyncio.Future()
# asyncio.ensure_future(slow_operation(future))
# # loop 的 run_until_complete 会将 _run_until_complete_cb 添加到 future 的完成回调列表中。而 _run_until_complete_cb 中会执行 loop.stop() 方法
# loop.run_until_complete(future)
# print(future.result())
# # 关闭事件循环对象
# loop.close()

# def thread_loop_task(loop, out_future):
    
#     # 为子线程设置自己的事件循环
#     asyncio.set_event_loop(loop)

#     async def work_2():
#         print('work_2 on loop:%s' % id(loop))
#         await asyncio.sleep(2)

#     async def work_4():
#         print('work_4 on loop:%s' % id(loop))
#         await asyncio.sleep(4)

#     future = asyncio.gather(work_2(), work_4())
#     loop.run_until_complete(future)
#     out_future.set_result('outFuture is done!')

# if __name__ == '__main__':

#     out_future = asyncio.Future()

#     # 创建一个事件循环thread_loop
#     thread_loop = asyncio.new_event_loop() 

#     # 将thread_loop作为参数传递给子线程
#     t = threading.Thread(target=thread_loop_task, args=(thread_loop,out_future))
#     t.daemon = True
#     t.start()

