'''
Created on May 19, 2012

@author: ivan
'''
import unittest
from persistent_queue import *
import os
import queue
import time
import random
import threading


class Counter():
    def __init__(self):
        self.l=threading.Lock()
        self.count=0
    def increase(self):
        with self.l:
            self.count+=1
    def __call__(self):
        with self.l:
            return self.count      
def rand_wait(): 
    r=random.gauss(0.1,0.3)
    if r<0: r=0
    if r>1: r=1
    return r


class Test(unittest.TestCase):

    def _del_files(self):
        
        
            for name in ('test', 'test.unfinished'):
                for ext in ('', '.bak', '.dat', '.dir'):
                    try:
                        os.remove(name+ext)
                    except:
                        pass
        
    def setUp(self):
        self._del_files()


    def tearDown(self):
        self._del_files()

    def testQ1(self):
        q=PersistentQueue('test')
        no=100
        for i in range(no):
            q.put(i)
        self.assertEqual(len(q.unfinished), 0)
        for i in range(no):
            x,id=q.get()
            self.assertEqual(x,i, 'same order')
            
            self.assertEqual(len(q.unfinished), 1)
            q.task_done(id)
        self.assertEqual(len(q.unfinished),0)
        
        q.close()
        
    def testQ2(self):
        q=PersistentQueue('test')
        for i in range(100):
            q.put(i)
            
        for i in range(10):
            x,tid=q.get()
            self.assertEqual(x,i)
            q.task_done(tid)
            
        for i in range(10,20):
            x,tid=q.get()
        self.assertEqual(q.qsize(), 80)
        self.assertEqual(len(q.unfinished), 10)
            
        q.close()
        q=PersistentQueue('test')    
            
        self.assertEqual(q.qsize(), 90)
        self.assertEqual(len(q.unfinished), 0)
        
        for i in range(10):
            x,tid=q.get()
            self.assertTrue(x in list(range(10,20)), 'Unfinished goes first')
            q.task_done(tid)
            
        for i in range(20,100):
            x,tid=q.get()
            self.assertEqual(x,i)
            q.task_done(tid)
            
        self.assertEqual(q.qsize(),0)
        q.close()
        
            
    def testFifo1(self):
        q=PersistentFIFO('test')
        no=1000
        self.assertEqual(len(q), 0, 'inital size is 0')
        for i in range(no):
            q.append(i)
        self.assertEqual(len(q), no, 'size is %d' % no)
        for i in range(no):
            x=q.popleft()
            self.assertEqual(x, i, 'got same number as put')
            
        self.assertEqual(len(q), 0, 'len is 0 again')
        q.close()
        
    def testFifo1a(self):
        q=PersistentFIFO('test')
        no=1000
        self.assertEqual(len(q), 0, 'inital size is 0')
        for i in range(no):
            q.appendleft(i)
        self.assertEqual(len(q), no, 'size is %d' % no)
        for i in range(no-1,-1,-1):
            x=q.popleft()
            self.assertEqual(x, i, 'got same number as put')
            
        self.assertEqual(len(q), 0, 'len is 0 again')
        q.close()
        
    def testFifo2(self):
        no=10
        q=PersistentFIFO('test', no)
        
        self.assertRaises(ErrorQueueEmpty, q.popleft)
            
        
        for i in range(no):
            q.append(i)
            
        self.assertRaises(ErrorOutOfCapacity, q.append, 'fail')
            
        q.close()
        
    def testFiFo3(self):
        q=PersistentFIFO('test')
        for i in range(10):
            q.append(i)
        q.close()
        q=PersistentFIFO('test')
        for i in range(10,20):
            q.append(i)
        self.assertEqual(len(q),20)
        
        for i in range(20):
            x=q.popleft()
            self.assertEqual(x,i)
            
        self.assertEqual(len(q), 0)
        
        q.close()
        
    
    def testFifo4(self):
        q=PersistentFIFO('test', 10)
        for i in range(1000):
            q.append(i)
            x=q.popleft()
            self.assertEqual(x,i)
        self.assertEqual(len(q),0)
        
    def testFifo4a(self):
        q=PersistentFIFO('test', 10)
        for i in range(1000):
            q.appendleft(i)
            x=q.popleft()
            self.assertEqual(x,i)
        self.assertEqual(len(q),0)
        
    def testTP1(self):
        def print_number(n):
            print('thread %s : %d' %(threading.current_thread().name,n))
        tp=ThreadPool('test', print_number, 5)
        for i in range(100):
            tp.add_task(i)
            
        tp.wait_completion()
        
        self.assertEqual(tp.tasks.qsize(),0)
        self.assertEqual(len(tp.tasks.unfinished),0)
        
        tp.close()
        
        
    def testTP2(self):
         
        c=Counter()
        def print_number(n):
            time.sleep(rand_wait())
            print('thread %s : %d' %(threading.current_thread().name,n))
            c.increase()
        
        tp=ThreadPool('test', print_number, 5)
        for i in range(100):
            tp.add_task(i)
        time.sleep(0.5)
        tp.stop()  
        tp.join()
        
        self.assertTrue(tp.tasks.qsize()>0)
        self.assertTrue(c()>0)
        self.assertEqual(len(tp.tasks.unfinished),0)
        
        tp.close()
        tp=ThreadPool('test', print_number, 5)
        
        tp.wait_completion()
        
        self.assertEqual(tp.tasks.qsize(),0)
        self.assertEqual(c(),100)
        self.assertEqual(len(tp.tasks.unfinished),0)
        
    def testTP3(self):
        
        c=Counter()       
        def print_number(n):
            
            time.sleep(rand_wait())
            print('thread %s : %d' %(threading.current_thread().name,n))
            c.increase()
            
        def stop_tp(tp):
            tp.stop()
        tp=ThreadPool('test', print_number, 5, max_queue_size=5)
        t=threading.Timer(0.5, stop_tp, args=[tp] )
        t.start()
        sent=0
        for i in range(100):
            try:
                tp.add_task(i)
                sent+=1
            except Interrupted:
                break
        
        tp.join()
        
        self.assertTrue(tp.tasks.qsize()>0)
        self.assertTrue(c()>0)
        self.assertEqual(len(tp.tasks.unfinished),0)
        
        tp.close()
        tp=ThreadPool('test', print_number, 5)
        
        tp.wait_completion()
        
        self.assertEqual(tp.tasks.qsize(),0)
        self.assertEqual(c(),sent)
        self.assertEqual(len(tp.tasks.unfinished),0)


if __name__ == "__main__":
    #import sys;sys.argv = ['','persistent_queue.Test.testTP2']
    unittest.main(argv=['', 'Test.testFifo2',])