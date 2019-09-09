import multiprocessing as mp
from multiprocessing import Process, Pool, Lock
import threading
import time
import os

# __all__ = ["XHandler", "Message"]

def info(zzz):
    pid = os.getpid()
    print(f'pid: {pid}', 1)
    time.sleep(zzz)
    print(f'pid: {pid}', 2)

class XHandlerException(Exception):
    pass

class Message():
    TYPE_APPLY = 'apply'
    TYPE_MAP = 'map'
    TYPE_MSG = 'message'
    def __init__(self, msg_type, *args, **kwargs):
        self.msg_type = msg_type
        self.args = args
        self.kwargs = kwargs

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
        self.commands = mp.Queue()
        self.queue = mp.Queue()
        self.waiting_in_queue = set()
        self.waiting_results = {}
        self.lock_for_queue = threading.Lock()
        # the user defined message handler
        self.message_hander = message_handler
        if workers is None or workers < 0:
            self.pool = Pool()
        else:
            self.pool = Pool(workers)

        def dispatch():
            # dispatcher to polling the task queue
            while(True):
                while self.commands.qsize() > 0:
                    msg = self.commands.get()

                if self.queue.qsize() > 0:

                    self.lock_for_queue.acquire()
                    msg = self.queue.get()
                    if msg.delay > 0:
                        cur_time = time.time()
                        # print('-'*10, cur_time, msg.posted_time + msg.delay)
                        if msg.posted_time + msg.delay > cur_time:
                            # print('delaying...')
                            self.queue.put(msg)
                            self.lock_for_queue.release()
                            continue
 
                    self.lock_for_queue.release()
                    self._execute_task(msg)

                    


        if mode == self.MODE_MULTI_PROCESSING:
            # start a dispatcher 
            dispatcher = threading.Thread(target=dispatch, name='xhandler_dispatcher', daemon=True)
            dispatcher.start()
            print('start dispatcher...')

    def _execute_command(self, cmd, *args, **kwargs):
        pass

    def setMessageHandler(self, func):
        self.message_hander = func

    def post(self, func, *args, callback=None, error_callback=None,
                identifier=None, sync_task=False, delay=0, future_wait=False, **kwds):
        '''
        target: the function to be executed.
        args: the parameters to be passed to target
        delay: the time delay in seconds to execute the target
        future_wait: might wait in the future, when set True, an unique identifier should be set
        '''

        if future_wait and identifier is None:
            raise XHandlerException('A unique identifier must be set when future_wait is True.')

        if sync_task:
            # TODO handle synchronous task
            self.pool.apply(func, args, kwds)
            return 

        msg = Message(Message.TYPE_APPLY)
        msg.func = func
        msg.args = args
        msg.callback = callback
        msg.error_callback = error_callback
        msg.identifier = identifier
        msg.delay = delay
        msg.future_wait = future_wait
        msg.kwds=kwds
        msg.posted_time = time.time()

        # self.queue.put(msg)
        # self.pool.apply_async(func, args)
        self._post(msg)


    def postMany(self, func, args=None, chunksize=None, callback=None, error_callback=None,
            args_parse='starmap', identifier=None, sync_task=False, delay=0,  future_wait=False):

        '''
        https://docs.python.org/3/library/multiprocessing.html#Pool
        
        args_parse: 'map', generate args then execute task;
                    'imap', like map but lazily generate args in chunksize;
                    'imap_unordered', similar to imap, but in arbitrary order, only support synchronous task
                    'starmap', automatically unpack args, e.g. an iterable of [(1,2), (3, 4)] results in [func(1,2), func(3,4)].
        '''

        if sync_task:
            # TODO support for other type
            if args_parse == 'map':
                 self.pool.map(func, args, chunksize)
            elif args_parse == 'imap':
                 self.pool.imap(func, args, chunksize)
            elif args_parse == 'imap_unordered':
                 self.pool.imap_unordered(func, args, chunksize)
            elif args_parse == 'starmap':
                 self.pool.starmap(func, args, chunksize)
            else:
                raise XHandlerException('Unsupported ')
           
            return

        if args_parse == 'imap_unordered' and not sync_task:
            raise XHandlerException("Only support 'imap_unordered' for synchronous tasks")

        msg = Message(Message.TYPE_MAP)
        msg.func = func
        msg.args = args
        msg.chunksize = chunksize
        msg.callback = callback
        msg.error_callback = error_callback
        msg.args_parse = args_parse
        msg.identifier = identifier
        msg.delay = delay
        msg.future_wait = future_wait
        msg.posted_time = time.time()

        self._post(msg)

        
    def postMessage(self, task, id=None, delay=0):
        '''
        id: the identifier
        '''
        pass

    def _post(self, msg):
        if msg.future_wait and msg.identifier is None:
            raise XHandlerException(
            'Unsupported arg parse method, only support: (map, imap, startmap) for asynchronous tasks')

        self.lock_for_queue.acquire()
        if msg.future_wait:
            self.waiting_in_queue.add(msg.identifier)
        self.queue.put(msg)
        # print('posted')
        self.lock_for_queue.release()
        # self.pool.map_async(func, args, chunksize=chunksize, callback=callback, error_callback=error_callback)


    def cancel(self, *identifier):
        for ident in identifier:
            self._cancel(ident)

    def _cancel(self, indentifier):
        # TODO cancel a task
        # remove from waiting in queue if possible
        # remove from queue
        pass

    def wait(self, identifiers=None):
        '''
        identifiers: an iteratable, contains the ids of waiting tasks
        '''
        if identifiers is None:
            while len(self.waiting_in_queue) > 0:
                continue
            # TODO to be optimized
            return {k: v.get() for k, v in self.waiting_results.items()}

        results = {}
        for k in identifiers:
            while k in self.waiting_in_queue:
                continue

            if k not in self.waiting_results:
                continue
            # this will wait the task to complete
            results[k] = self.waiting_results[k].get()
        return results

    def terminate(self):
        '''
        Terminate the handler and close the workers pool. 
        CAUTION: all executing task will be 
        '''
        # terminate all task immediately
        self.pool.terminate()

    def close_join(self):
        '''
        This method might block the thread until all task completed
        '''
        while self.queue.qsize() > 0:
            continue

        self.pool.close() # waiting for all task to complete
        self.pool.join()

    def _get_map_method(self, args_parse):
            if args_parse == 'map':
                return self.pool.map_async
            elif args_parse == 'imap':
                return self.pool.imap_async
            # elif msg.args_parse == 'imap_unordered':
            #     return self.pool.imap_unordered_async
            elif args_parse == 'starmap':
                return self.pool.starmap_async
            else:
                # 'imap_unordered' is only supported for synchronous task
                raise XHandlerException(
            'Unsupported arg parse method, only support: (map, imap, startmap) for asynchronous tasks')


    def _execute_task(self, msg):
        self.lock_for_queue.acquire()
        # print('executing....')
        if msg.future_wait and msg.identifier in self.waiting_in_queue:
            self.waiting_in_queue.remove(msg.identifier)

        if msg.msg_type == Message.TYPE_APPLY:
            result = self.pool.apply_async(msg.func, msg.args, msg.kwds, msg.callback, msg.error_callback)
            # print('getting result...1', os.getpid())
            # result.get()
            if msg.future_wait:
                self.waiting_results[msg.identifier] = result
        elif msg.msg_type == Message.TYPE_MAP:
            async_method = self._get_map_method(msg.args_parse)
            result = async_method(msg.func, msg.args, msg.chunksize, msg.callback, msg.error_callback)
            # print('getting result...')
            if msg.future_wait:
                self.waiting_results[msg.identifier] = result
            # if msg.args_parse == 'map':
            #     self.pool.map_async(msg.func, msg.args, msg.chunksize, msg.callback, msg.error_callback)
            # elif msg.args_parse == 'imap':
            #     self.pool.imap_async(msg.func, msg.args, msg.chunksize, msg.callback, msg.error_callback)
            # elif msg.args_parse == 'starmap':
            #     self.pool.starmap_async(msg.func, msg.args, msg.chunksize, msg.callback, msg.error_callback)
            # else:
            #     raise XHandlerException(
            # 'Unsupported arg parse method, only support: (map, imap, startmap) for asynchronous tasks')
        elif msg.msg_type == Message.TYPE_MSG:
            self.pool.apply_async(self.message_hander, args=msg.args)

        self.lock_for_queue.release()



_root_xhandler = None
def getHandler(mode=XHandler.MODE_MULTI_PROCESSING, workers=-1, message_handler=None):
    # TODO use a manager
    global _root_xhandler
    if _root_xhandler is None:
        _root_xhandler = XHandler(mode=mode, workers=workers, message_handler=message_handler)
    
    return _root_xhandler
