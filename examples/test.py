import sys
sys.path.insert(0, "../../")

import multiprocessing as mp
from multiprocessing import Pool
import os
import time
import xhandler

def info(zzz, hhh):
    pid = os.getpid()
    print(f'pid: {pid}', 1, zzz, hhh)
    time.sleep(zzz)
    print(f'pid: {pid}', 2)
    return 'hahaha'+str(zzz)


# with Pool() as pool:
#     pool.map()
#     pool.

if __name__ == '__main__':
    handler = xhandler.getHandler()
    for i in range(10):
        handler.post(info, i, 2, future_wait=True, identifier='test'+str(i))
    handler.post(info, 5, 3, delay=10)
    # print('Waiting for joining...', os.getpid())
    # time.sleep(3)
    # handler.terminate()
    print('Waiting...')
    results = handler.wait(('test9',))
    print('results:', results)
    print('To join')
    # handler.wait()
    handler.close_join()
    # handler.terminate()
    print('Joined')

    # pool = Pool()
    # result = pool.apply_async(info, (0,0))
    #     # for i in range(10):
    #     #     pool.apply_async(info, (1,))

    # print('.....')
    # time.sleep(2)
    # # result.wait()
    # # pool.close(0)
    # # pool.join()
    # print(result.get())
    # print(result.get())
    # print('joined')
# if __name__ == '__main__':
#     pool = Pool()
#     for i in range(10):
#         pool.apply_async(info, (i,))

#     pool.close()
#     pool.join()
