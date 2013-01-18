#! /usr/bin/env python3
'''
Created on May 11, 2012

@author: ivan
'''

import logging
from optparse import OptionParser
import sys
import  os,os.path
import signal
import functools
import time
import threading
import pwd
import grp

from httputils import HTTPClient
from persistent_queue import ThreadPool, Interrupted
from daemon import DaemonContext


plugin_points=['START_URL', 'REPEATS', 'MEAN_WAIT', 'MAX_WAIT',
               'REPEATS2', 'MEAN_WAIT2', 'MAX_WAIT2', 'MAX_QUEUE_SIZE', 
               'DOWN_THREADS', 
               'save_file', 'MySpider']


class PidContext(): 
    class AlreadyRunningError(Exception): 
        pass
    def __init__(self, ro=False):
        self.ro=ro
        name='/tmp/downloader.py.pid'
        
        self.pid_file_name=name
        
        if ro:
            return
        if not self._is_running():
            self.pid_file=open(self.pid_file_name,'w')
        else:
            raise self.AlreadyRunningError()
        
        
    def __enter__(self):
        if self.ro:
            raise RuntimeError("Object is readonly")
        self.pid_file.write(str(os.getpid()))
        self.pid_file.flush()
    def __exit__(self, exc_type, exc_value, traceback):
        self.pid_file.close()
        try:
            os.remove(self.pid_file_name)
        except:
            pass
        
    def fileno(self):
        return self.pid_file.fileno()
    
    def _is_running(self):
        return self.signal(0)
        
    def signal(self, signal):
        if os.path.exists(self.pid_file_name):
            with open(self.pid_file_name, 'r') as pid_file:
                pid=pid_file.read()
                if not pid:  
                    return
                pid=int(pid)
            try:
                os.kill(pid, signal)
                return True
            except :
                pass            

def interrupt(*args):
    for stopable in args:
        stopable.stop()
    logging.info('Interrupting program by timer')
#    time.sleep(180) # give some timeout to finish threads
#    logging.error('Programm did not finish, terminating now')
#    os.kill(os.getpid(), signal.SIGTERM)
    
def init (plugin, base_dir, options):
    logging.basicConfig()
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
    
def run(plugin, base_dir, options):
    
    
    pool=None
    try:
        client=HTTPClient(options.proxy, options.no_proxy, plugin.REPEATS,plugin.MEAN_WAIT, plugin.MAX_WAIT)
        client2=HTTPClient(options.proxy, options.no_proxy,plugin.REPEATS2,plugin.MEAN_WAIT2, plugin.MAX_WAIT2)
        pool=ThreadPool(os.path.join(base_dir, 'pool_items'), functools.partial(plugin.save_file, client2),
                         plugin.DOWN_THREADS,options.resume, plugin.MAX_QUEUE_SIZE)
        timer=None
        if options.stop_after:
            timer=threading.Timer(int(options.stop_after)*60, interrupt, args=[pool, client, client2])
            timer.daemon=True
            timer.start()
        def stop_now(*args):
            interrupt(pool, client, client2)
        signal.signal(signal.SIGUSR1, stop_now)
        
        spider=plugin.MySpider(client, plugin.START_URL, 
                        os.path.join(base_dir, 'last_page.txt') if options.resume else None )
        try:
            for link, metadata in spider:
                logging.debug('Got id %s' % metadata['id'])
                pool.add_task(link, metadata, base_dir)
        except Interrupted:
            logging.info('Main loop interrupted - leaving')
            
        try:
            pool.wait_completion(not options.daemon)
            logging.debug("Pool should finish all tasks - %d" % pool.tasks.unfinished_tasks)
        except Interrupted:
            logging.info('Waiting for threads to complete')
            pool.join(not options.daemon)
    except Interrupted:
        logging.info("Interruped at early stage")
    except Exception:
        logging.exception("Downloader run exceeds with error")
        return True
    except (SystemExit, KeyboardInterrupt) as e:
        logging.info("terminating by %s", e)
        if pool:
            pool.close()
        raise e
    finally:
        signal.signal(signal.SIGUSR1, signal.SIG_IGN)
        
    
def main(): 
    signal.signal(signal.SIGUSR1, signal.SIG_IGN)
    if len(sys.argv)>1:  
        pid=PidContext(True)
        if sys.argv[1]=='--kill':
            if pid.signal(signal.SIGTERM):
                print("Send terminate signal")
                return
            else:
                print("Daemon not running")
                return 
        elif sys.argv[1]=='--stop':
            if pid.signal(signal.SIGUSR1):
                print("Send stop current cycle  signal")
                return
            else:
                print("Daemon not running")
                return 
            
              
    try:
        plugin=sys.argv[1]
    except :
        print('first argument must be plugin name !', file=sys.stderr)
        sys.exit(1)
    plug_module='plugins.'+plugin
    __import__(plug_module)
    plugin=sys.modules[plug_module]
    for name in plugin_points:
        if not hasattr(plugin, name):
            print('Invalid plugin - missing attribute %s'% name,  file=sys.stderr)  
            sys.exit(4)  
        
    opt_parser=OptionParser("%s plugin|--kill|--stop options directory_to_store" %sys.argv[0])
    opt_parser.add_option('--proxy', help="HTTP proxy to use, otherwise system wise setting will ")
    opt_parser.add_option('--no-proxy', dest='no_proxy', action='store_true', help='Do not use proxy, even if set in system')
    opt_parser.add_option('-r', '--resume', action='store_true', help='Resumes form from last run - starts on last unfinished page '+
                          'and restores queue of unfinished downloads')
    opt_parser.add_option('-s', '--stop-after', dest='stop_after', type='int', help='Stop approximatelly after x minutes (valid only if not daemon)')
    opt_parser.add_option('-c', '--continue-after', dest='continue_after', type='int', help='Continue approximatelly after x minutes (valid only for daemon)')
    opt_parser.add_option('-d', '--debug', action='store_true', help='Debug logging')
    opt_parser.add_option('--daemon', action='store_true', help='Run as daemon')
    opt_parser.add_option('--user',  help='Run under this user')
    opt_parser.add_option('--group',  help='Run under this group')
    opt_parser.add_option('-l', '--log', help="Log file")
    
    if hasattr(plugin, 'OPTIONS'):
        opt_parser.add_options(plugin.OPTIONS)
    options,args=opt_parser.parse_args(sys.argv[2:])
    
    if len(args)<1:
        opt_parser.print_help()
        sys.exit(2)    
    
    base_dir=args[0]
    if not os.path.exists(base_dir):
        print('The output directory %s does not exists, exiting' % base_dir, file=sys.stderr)
        sys.exit(3)
    if options.daemon:
        logging.info("Started daemon with encoding %s", sys.getdefaultencoding())
        def get_id(name, fn):
            if not name:
                return
            try:
                return fn(name)[2]
            except KeyError:
                pass
        
        options.stop_after=0
        try:
            pid=PidContext()
        except PidContext.AlreadyRunningError:
            print("Downloader is already running", file=sys.stderr)
            sys.exit(5)
        uid= get_id(options.user, pwd.getpwnam)
        gid = get_id(options.group, grp.getgrnam)
        
        context_args={'uid':uid, 
                      'gid':gid, 
                      'working_directory':os.getcwd(),
                      'pidfile':pid,
                      'files_preserve':[pid]}
        #context_args.update({'stdout': sys.stdout, 'stderr':sys.stderr})
        with DaemonContext(**context_args):
            init(plugin, base_dir, options)
            errors=0
            while True:
                was_error=run(plugin, base_dir, options)  
                if not was_error:
                    errors=0
                    to_sleep=int(options.continue_after or 0)*60 or 3600
                    logging.info("Finished a run, will wait %d", to_sleep)
                else:
                    errors+=1
                    limit=10
                    if errors>limit:
                        logging.error('Limit of %d fatal errors reach - stopping daemon', limit)
                        break
                    to_sleep=plugin.MAX_WAIT
                    logging.info("Finished with error will retry after %d", to_sleep)
                time.sleep(to_sleep)
    else:   
        init(plugin, base_dir, options)
        run(plugin, base_dir, options)
        
if __name__=='__main__':
    main()