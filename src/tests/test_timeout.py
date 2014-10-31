'''
Created on Oct 30, 2014

@author: ivan
'''
import unittest
import subprocess
from httputils import HTTPClient
import time
import socket



    

class Test(unittest.TestCase):
    
    def run_server(self,args):
        cmd=['python3', 'lousy_server.py']
        cmd.extend(args)
        p=subprocess.Popen(cmd)
        time.sleep(1)
        self.assertTrue(p.pid>0)
        return p
        
    def stop_server(self, p):
        p.kill()
        time.sleep(1)
        
    def test_ok(self):
        p=self.run_server([])
        c=HTTPClient(timeout=2)
        res = c.open_url('http://localhost:5000')
        data = res.read()
        self.assertTrue(len(data)>1000)
        self.stop_server(p)

    def test_head(self):
        p=self.run_server(['--header-delay', '5'])
        c=HTTPClient(timeout=2)
        try:
            res = c.open_url('http://localhost:5000')
            self.fail('Should raise error')
        except HTTPClient.Error: 
            pass
        self.stop_server(p)
        
    def test_read(self):
        p=self.run_server(['--send-delay', '5'])
        c=HTTPClient(timeout=2)
        res = c.open_url('http://localhost:5000')
        try:
            data = res.read()
            self.fail('Should raise error')
        except socket.timeout: 
            pass
        self.stop_server(p)
       
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test']
    unittest.main()