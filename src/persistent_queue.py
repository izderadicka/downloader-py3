'''
Created on May 18, 2012

@author: ivan
'''

import shelve
import sys
import os
import uuid
# dbm.gnu and dbm.dump basically behaves fine if close is not called, ndbm loose data
try:
    import dbm.gnu as db
except:
    print('Should install pyhton3-gdbm', file=sys.stderr)
    import dbm.dumb as db
from time import sleep
import signal
from time import time as _time
from queue import Full, Empty, Queue
import logging
import threading
log=logging.getLogger('persistent_queue')
MAXINT=sys.maxsize


class ErrorOutOfCapacity(MemoryError): pass
class ErrorQueueEmpty(IndexError): pass
class Interrupted(Exception): pass

class PersistentFIFO():
    def __init__(self,filename,  size=None, cached=True):
        self.db=shelve.Shelf(db.open(filename, "c"),  writeback=cached)
        self._init_indexes()
        if not size:
            self.limit=MAXINT
        else:
            self.limit=size
        
    def _init_indexes(self):
        def _init_index(name):
            if self.db.get(name, None) is None:
                setattr(self, name, 0)
            else:
                setattr(self, name, self.db[name])
        _init_index('top')
        _init_index('bottom')
        
    def __len__(self):
        if self.top>=self.bottom:
            return self.top-self.bottom
        else:
            return self.limit-self.bottom +1 +self.top
    
    def append(self, item):
        
        self.db[str(self.top)]=item
        self.top+=1
        if self.top > self.limit:
            self.top=0
        if self.top==self.bottom:
            raise ErrorOutOfCapacity
        self.db['top']=self.top
        
    
    def popleft(self):
        if self.top==self.bottom:
            raise ErrorQueueEmpty('Queue is empty')
        val=self.db[str(self.bottom)]
        old_index=self.bottom
        self.bottom+=1
        if self.bottom>self.limit:
            self.bottom=0
        self.db['bottom']=self.bottom
        del self.db[str(old_index)]
        return val
    
    def peekleft(self):
        if self.bottom==self.top:
            return None
        return self.db[str(self.bottom)]
    
    def appendleft(self,item):
        idx=self.bottom-1
        if idx<0:
            idx=self.limit
        self.db[str(idx)]=item
        if idx==self.top:
            raise ErrorOutOfCapacity
        self.bottom=idx
        self.db['bottom']=idx
        
    
    def close(self):
        self.db.close()
        
class PersistentQueue(Queue): 
    def __init__(self, filename, maxsize=0, resume=True): 
        self.filename=filename
        self.resume=resume
        super(PersistentQueue,self).__init__(maxsize)
        if self.resume:
            self._restore_unfinished()
        
    def _init(self, maxsize):
        #TODO:  Change - this will not work with dummy dbm
        if not self.resume:
            for ext in ('', '.bak', '.dat', '.dir'):
                try:
                    os.remove(self.finame+ext)
                except:
                    pass
                
        self.queue = PersistentFIFO(self.filename)
        self.unfinished=shelve.Shelf(db.open(self.filename+'.unfinished', "c"))
        self.stop=False
       
            
    def _restore_unfinished(self):
        self.unfinished_tasks+=len(self.queue)
        tbd=[]
        for id in self.unfinished:
            self.queue.appendleft(self.unfinished[id])
            self.unfinished_tasks+=1
            tbd.append(id)
        for id in tbd:
            del self.unfinished[id]

    

    def _get(self):
        #TODO - Keep track of unfinished items
        item=self.queue.peekleft() 
        _uuid=str(uuid.uuid4())
        self.unfinished[_uuid]=item
        self.queue.popleft() 
        return item, _uuid
    
    def interrupt(self):
        with self.mutex:
            self.stop=True
            self.not_full.notify_all()
            self.not_empty.notify_all()
            self.all_tasks_done.notify_all()
        
    
    def task_done(self, item_id=None):
        """Indicate that a formerly enqueued task is complete.

        Used by Queue consumer threads.  For each get() used to fetch a task,
        a subsequent call to task_done() tells the queue that the processing
        on the task is complete.

        If a join() is currently blocking, it will resume when all items
        have been processed (meaning that a task_done() call was received
        for every item that had been put() into the queue).

        Raises a ValueError if called more times than there were items
        placed in the queue.
        """
        self.all_tasks_done.acquire()
        try:
            unfinished = self.unfinished_tasks - 1
            if item_id:
                try:
                    del self.unfinished[item_id]
                except KeyError:
                    log.error("Task ID %s is not found in unfinished" % str(item_id))
            if unfinished <= 0:
                if unfinished < 0:
                    raise ValueError('task_done() called too many times')
                self.all_tasks_done.notify_all()
            self.unfinished_tasks = unfinished
        finally:
            self.all_tasks_done.release()
            
    def put(self, item, block=True, timeout=None):
        """Put an item into the queue.

        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until a free slot is available. If 'timeout' is
        a positive number, it blocks at most 'timeout' seconds and raises
        the Full exception if no free slot was available within that time.
        Otherwise ('block' is false), put an item on the queue if a free slot
        is immediately available, else raise the Full exception ('timeout'
        is ignored in that case).
        """
        self.not_full.acquire()
        try:
            if self.stop:
                raise Interrupted
            if self.maxsize > 0:
                if not block:
                    if self._qsize() == self.maxsize:
                        raise Full
                elif timeout is None:
                    while self._qsize() == self.maxsize:
                        self.not_full.wait()
                        if self.stop:
                            raise Interrupted
                elif timeout < 0:
                    raise ValueError("'timeout' must be a positive number")
                else:
                    endtime = _time() + timeout
                    while self._qsize() == self.maxsize:
                        remaining = endtime - _time()
                        if remaining <= 0.0:
                            raise Full
                        self.not_full.wait(remaining)
                        if self.stop:
                            raise Interrupted
            self._put(item)
            self.unfinished_tasks += 1
            self.not_empty.notify()
        finally:
            self.not_full.release()
            
    def get(self, block=True, timeout=None):
        """Remove and return an item from the queue.

        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until an item is available. If 'timeout' is
        a positive number, it blocks at most 'timeout' seconds and raises
        the Empty exception if no item was available within that time.
        Otherwise ('block' is false), return an item if one is immediately
        available, else raise the Empty exception ('timeout' is ignored
        in that case).
        """
        self.not_empty.acquire()
        try:
            if self.stop:
                raise Interrupted
            if not block:
                if not self._qsize():
                    raise Empty
            elif timeout is None:
                while not self._qsize():
                    self.not_empty.wait()
                    if self.stop:
                        raise Interrupted
            elif timeout < 0:
                raise ValueError("'timeout' must be a positive number")
            else:
                endtime = _time() + timeout
                while not self._qsize():
                    remaining = endtime - _time()
                    if remaining <= 0.0:
                        raise Empty
                    self.not_empty.wait(remaining)
                    if self.stop:
                        raise Interrupted
            item = self._get()
            self.not_full.notify()
            return item
        finally:
            self.not_empty.release()
            
    def join(self):
        """Blocks until all items in the Queue have been gotten and processed.

        The count of unfinished tasks goes up whenever an item is added to the
        queue. The count goes down whenever a consumer thread calls task_done()
        to indicate the item was retrieved and all work on it is complete.

        When the count of unfinished tasks drops to zero, join() unblocks.
        """
        self.all_tasks_done.acquire()
        try:
            if self.stop:
                raise Interrupted
            while self.unfinished_tasks:
                self.all_tasks_done.wait()
                if self.stop:
                    raise Interrupted
        finally:
            self.all_tasks_done.release()
            
    def close(self):
        self.queue.close()
        self.unfinished.close()
        
        
class Worker(threading.Thread):
    
    def __init__(self, tasks, callable, name=None):
        super(Worker,self).__init__(name=name)
        self.tasks = tasks
        self.daemon = True
        self.running= True
        self.callable=callable
        self.start()
    
    def run(self):
        while self.running:
            try:
                args, tid = self.tasks.get()
            except Interrupted:
                log.info('Queue interrrupted, exiting thread %s' %threading.current_thread().name)
                break
            try: 
                self.callable(*args)
            except Exception: 
                log.exception('Exception while running thread')
            self.tasks.task_done(tid)
    def stop(self):
        self.running=False
        

class ThreadPool:
    """Pool of threads consuming tasks from a queue"""
    def __init__(self, filename, callable, num_threads, resume=True, max_queue_size=0):
        self.tasks = PersistentQueue(filename, maxsize=max_queue_size, resume=resume)
        self.threads=[]
        for i in range(num_threads): 
            self.threads.append(Worker(self.tasks,callable, 'Worker %d'% i))

    def add_task(self,  *args):
        """Add a task to the queue 
        can throw interrupted if stop is called"""
        
        self.tasks.put(args)

    def wait_completion(self, close_on_complete=False):
        """Wait for completion of all the tasks in the queue"""
        self.tasks.join()
        if close_on_complete:
            self.close()
            
    def join(self, close_on_complete=False):
        for t in self.threads:
            t.join()
        if close_on_complete:
            self.close()
        
    def stop(self):
        for t in self.threads:
            t.stop()
        self.tasks.interrupt()
            
    def close(self):
        self.tasks.close()
            
if __name__ == '__main__':
    pass


#        d=shelve.Shelf(db.open("pstore", "c"), writeback=True)
#        def on_exit(sig, frame):
#            print('terminating program')
#            d.close()
#        #signal.signal(signal.SIGTERM, on_exit)
#        if d.get('top'):
#            
#            top=int(d['top'])
#            print('restoring index to %d' % top  )
#        else:
#            top=1
#            
#        while True:
#            d[str(top)]=top
#            print('stored %d' % top)
#            top+=1
#            d['top']=top
#            #d.sync()
#            sleep(1)
#        