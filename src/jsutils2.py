'''
Created on Apr 21, 2013

@author: ivan
'''


import subprocess
import tempfile
import os, os.path
import logging


class JSEngine(object):
    def __init__(self, script, page, link, timeout=120):
        if not script.startswith(os.sep):
            self.script=os.path.join(os.path.split(__file__)[0], script)
        else:
            self.script=script
        base_tag=page.new_tag('base', href=link);
        page.head.insert(0, base_tag)
        self.page=page
        self.timeout=timeout
        
    def eval(self):
        f,fname=tempfile.mkstemp('.html',  text=True)
        f=os.fdopen(f,'w')
        with f:
            f.write(str(self.page))
        try:
            #p=subprocess.Popen()
            res=subprocess.check_output(['phantomjs', '--load-images=no', 
                '--disk-cache=true', '--max-disk-cache-size=10000', self.script, fname], timeout=self.timeout)
            res=str(res, 'UTF8').strip()
            if len(res) and res.startswith('http'):
                return res
        except Exception as e:
            logging.exception('Error parsing JS')
        finally:
            os.remove(fname)
            
        
        