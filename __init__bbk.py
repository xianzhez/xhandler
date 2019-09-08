import multiprocessing as mp
from multiprocessing import Process, Pool, Lock
import time
import os

# __all__ = ["XHandler", "Message"]


class XHandler():
    MODE_MULTI_PROCESSING = 'multiprocessing'
    MODE_MULTI_THREADING = 'multithreading'
    

    def __init__(self, mode=MODE_MULTI_PROCESSING, workers=-1, message_handler=None):
        '''
        mode: XHandler.MODE_MULTI_PROCESSING uses multiprocessing to execute tasks to get rid of the constraints of GIL;
            XHandler.MODE_MULTI_THREADING uses multithreading, which is suitable for excuting tons of trivial short tasks.

        workers: the number of working processes or threads; value less than 0 will use the number of available cores.
        message_handler: a user defined handler for executing simple task conveniently.
        '''
        self.mode = mode
        # the queue to cache tasks
        self.queue = mp.Queue()
        # the user defined message handler
        self.message_hander = message_handler

        if workers is None or workers < 0:
            self.pool = Pool()
        else:
            self.pool = Pool(workers)


    def setMessageHandler(self, func):
        self.message_hander = func

    def post(self, target, args=None, identifier=None, callback=None, error_callback=None, delay=0, async_task=True):
        '''
        target: the function to be executed.
        args: the parameters to be passed to target
        delay: the time delay in seconds to execute the target
        '''

        self.pool.apply_async(target, args)


    def postMany(self, targets, argslist=None, args_generator=None, callback=None,
            error_callback=None):
        # TODO runnable batch
        self.pool.map_async(targets, argslist, callback=callback, error_callback=error_callback)

    def postMessage(self, task, id=None, delay=0):
        '''
        id: the identifier
        '''
        pass

    def join(self):
        # self.pool.close()
        # self.pool.join()
        time.sleep(20)
        

    def terminate(self):
        '''
        Terminate the handler and close the workers pool. 
        CAUTION: Any future posted task will result in unexpected exception after terminated.
        '''
        self.pool.close()
        self.pool.map()

    def _execute_task(self, msg):
        
        if msg.msg_type == Message.TYPE_RUNNABLE:
            print(msg.args)
            # self.pool.apply_async(msg.func, args=msg.args, callback=msg.callback, error_callback=msg.error_callback)
            self.pool.apply_async(info, (1,))
            # self.pool.apply(msg.func, args=msg.args)
            print('_handleMessage runnable')
        elif msg.msg_type == Message.TYPE_MSG:
            self.pool.apply_async(self.message_hander, args=msg.args)



_root_xhandler = None
def getHandler(mode=XHandler.MODE_MULTI_PROCESSING, workers=-1, message_handler=None):
    # use a manager
    global _root_xhandler
    if _root_xhandler is None:
        _root_xhandler = XHandler(mode=mode, workers=workers, message_handler=message_handler)
    
    return _root_xhandler
