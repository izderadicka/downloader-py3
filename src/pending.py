'''
Created on Jan 9, 2013

@author: ivan
'''
import sys
import shelve
import os.path
from persistent_queue import PersistentFIFO
try:
    import dbm.gnu as db
except:
    print('Should install pyhton3-gdbm', file=sys.stderr)
    import dbm.dumb as db
    
def print_items(q): 
    for i, k in enumerate(q):
        print ("%d : %s :%s"% (i,k, q.get(k)[0] if hasattr(q.get(k), '__len__') else q.get(k)))   

def print_items2(q): 
    for i, k in enumerate(q):
        print ("%d : %s :%s"% (i,k, k[0]))  
    
if __name__ == '__main__':
    if  len(sys.argv)<3:
        print("must provide data dir and queue size")
        sys.exit(1)
    base_dir=sys.argv[1]
    size= int(sys.argv[2])
    
    filename=os.path.join(base_dir, 'pool_items')
    queue=PersistentFIFO(filename, size)
    unfinished=shelve.Shelf(db.open(filename+'.unfinished', "c"))
    
    print("Queue size %d" %len(queue))
    print("Unfinished size %d" %len(unfinished))
    
    if len(queue):
        print("Queue items:")
        print_items(queue.db)   
        
        
        
    if len(unfinished):
        print("Unfinished items:")
        print_items(unfinished)   

    queue.close()
    unfinished.close()
    
   
        