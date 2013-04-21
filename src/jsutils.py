'''
Created on Apr 11, 2013

@author: ivan
'''

import PyV8
import types
import threading, logging
DUMMY_DOM="""var document={};
document.elements={}
document.getElementById= function(id) {
if (! this.elements[id] ) {
this.elements[id]={}
}
return this.elements[id]
};
"""

class ExecThread(threading.Thread):
    def __init__(self, script, result_expr):
        threading.Thread.__init__(self,name='PyV8')
        self.script=script
        self.result_expr=result_expr
     
        self.daemon=True
        self.result=None
        
    def run(self):
        with PyV8.JSContext() as ctx:
            try:
                ctx.eval(self.script)
                self.result=ctx.eval(self.result_expr)
            except: 
                logging.exception('JS error for script %s'% self.script)
                
    def eval(self):
        with PyV8.JSLocker():
            self.start()
        self.join(10)
        
        return self.result

class JSEngine(object):
    def __init__(self, final_expr, dummy_dom=True):
        self.final_expr=final_expr;
        self.dummy_dom=dummy_dom;
                
    def _run_js(self, script):
        t=ExecThread(script, self.final_expr)
        return t.eval()
        
    def eval_script(self, script_elem):
        
        if not isinstance(script_elem, str) and hasattr(script_elem, 'string'):
            script=script_elem.string
        else:
            script=script_elem
            
        if self.dummy_dom:
            script=DUMMY_DOM+'\n'+script;
        return self._run_js(script)
        
    
    
   
        
    