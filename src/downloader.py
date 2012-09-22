'''
Created on May 11, 2012

@author: ivan
'''

# Multithreaded
# Random pauses
# Random session lenghts


import logging
from optparse import OptionParser
import sys
import  os,os.path
import signal
import functools
import time
import threading
import imp

from httputils import HTTPClient
from persistent_queue import ThreadPool, Interrupted

logging.basicConfig()




plugin_points=['START_URL', 'REPEATS', 'MEAN_WAIT', 'MAX_WAIT',
               'REPEATS2', 'MEAN_WAIT2', 'MAX_WAIT2', 'MAX_QUEUE_SIZE', 
               'DOWN_THREADS', 
               'save_file', 'MySpider']



stop=False

def interrupt(tp):
    tp.stop()
    global stop
    stop=True
    logging.info('Interrupting program by timer')
    time.sleep(180) # give some timeout to finish threads
    logging.error('Programm did not finish, terminating now')
    os.kill(os.getpid(), signal.SIGTERM)
    
    
    
def main():        
    start=time.time()
    try:
        plugin=sys.argv[1]
    except :
        print('first argument must be plugin name!', file=sys.stderr)
        sys.exit(1)
    plug_module='plugins.'+plugin
    __import__(plug_module)
    plugin=sys.modules[plug_module]
    for name in plugin_points:
        if not hasattr(plugin, name):
            raise RuntimeError('Invalid plugin - missing attribute %s'% name)    
        
    opt_parser=OptionParser("%s plugin options directory_to_store" %sys.argv[0])
    opt_parser.add_option('--proxy', help="HTTP proxy to use, otherwise system wise setting will ")
    opt_parser.add_option('--no-proxy', dest='no_proxy', action='store_true', help='Do not use proxy, even if set in system')
    opt_parser.add_option('-r', '--resume', action='store_true', help='Resumes form from last run - starts on last unfinished page '+
                          'and restores queue of unfinished downloads')
    opt_parser.add_option('-s', '--stop-after', dest='stop_after', type='int', help='Stop approximatelly after x minutes')
    opt_parser.add_option('-d', '--debug', action='store_true', help='Debug logging')
    opt_parser.add_option('-l', '--log', help="Log file")
    
    if hasattr(plugin, 'OPTIONS'):
        opt_parser.add_options(plugin.OPTIONS)
    options,args=opt_parser.parse_args(sys.argv[2:])
    
    if len(args)<1:
        opt_parser.print_help()
        sys.exit(2)
    base_dir=args[0]
    
    if options.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    if options.log:
        h=logging.FileHandler(options.log)
        h.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logging.getLogger().addHandler(h)
    
    plugin.options=options
    plugin.BASE_DIR=base_dir
    if hasattr(plugin, 'init_plugin'):
        plugin.init_plugin()
    
    client=HTTPClient(options.proxy, options.no_proxy, plugin.REPEATS,plugin.MEAN_WAIT, plugin.MAX_WAIT)
    
    #http://xtrance.info/new/?mainpage=ebk&subpage=&id=&ebk_sort=14D
      
    spider=plugin.MySpider(client, plugin.START_URL, 
                    os.path.join(base_dir, 'last_page.txt') if options.resume else None )
    
    client2=HTTPClient(options.proxy, options.no_proxy,plugin.REPEATS2,plugin.MEAN_WAIT2, plugin.MAX_WAIT2)
    
    pool=ThreadPool(os.path.join(base_dir, 'pool_items'), functools.partial(plugin.save_file, client2),
                     plugin.DOWN_THREADS,options.resume, plugin.MAX_QUEUE_SIZE)
    timer=None
    if options.stop_after:
        timer=threading.Timer(int(options.stop_after)*60, interrupt, args=[pool])
        timer.daemon=True
        timer.start()
    
    for link, metadata in spider:
        logging.debug('Got id %s' % metadata['id'])
        try:
            pool.add_task(link, metadata, base_dir)
        except Interrupted:
            logging.info('Queue interrupted - leaving links parsing')
            break
        
    try:
        pool.wait_completion(True)
    except Interrupted:
        logging.info('Waiting for threads to complete')
        pool.join(True)
        
        
if __name__=='__main__':
    main()