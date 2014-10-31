'''
Created on Oct 29, 2014

@author: ivan
'''

from http.server import HTTPServer, BaseHTTPRequestHandler
import time
import sys
import logging
import urllib
from urllib.parse import urlparse
import os
import argparse
logger= logging.getLogger()

class LousyServer(HTTPServer):
    def __init__(self, address, handler_class, args):
        HTTPServer.__init__(self,address,handler_class)
        self._running=True
        self.args=args
        
    def stop(self):
        self._running=False
                
    def serve(self, w=0.1):
        while self._running:
            try:
                self.handle_request()
                time.sleep(w)
            except Exception as e:
                logger.exception("Server internal error")
                print( str(e), file=sys.stderr)
            
        
class LousyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.do_HEAD(only_header=False):
            sent=0
            waited=False
            with open(__file__,'rb') as f:
                while True:
                    if self.server.args.send_delay and sent >= 1000 and not waited:
                        
                        time.sleep(self.server.args.send_delay)
                        waited=True
                    buf= f.read(10)
                    if buf:
                        if logger.level<= logging.DEBUG:
                            logger.debug('Sending %d bytes', len(buf))
                        self.wfile.write(buf)
                        sent+=len(buf)
                    else:
                        if logger.level<= logging.DEBUG:
                            logger.debug('Finished sending data')
                        break
             
    def do_HEAD(self, only_header=True):
        parsed_url=urllib.parse.urlparse(self.path)
        if urllib.parse.unquote_plus(parsed_url.path)=='/':
            size,mime = os.stat(__file__).st_size, 'application/octet-stream'
            self.send_resp_header(mime, size, only_header)
            return True
        else:
            logger.error('We serve only root / path')
            self.send_error(404, 'Not Found')
        
    def send_resp_header(self, cont_type, cont_length, only_header=False):
        if self.server.args.header_delay:
            time.sleep(self.server.args.header_delay)
        self.send_response(200, 'OK')
        self.send_header('Content-Type', cont_type)
        self.send_header('Content-Length', cont_length)
        self.send_header('Connection', 'close')
        self.send_header('Accept-Ranges', 'none')
        if not only_header: self.end_headers()
        
        
def main(args):
    s=LousyServer(('127.0.0.1',5000), LousyHandler, args)   
    s.serve()     
    
if __name__ == '__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--send-delay', type=float, help="Number of secs to wait in the middle of file sending")
    p.add_argument('--header-delay', type=float, help="Number of secs to wait before sending header")
    args=p.parse_args()
    main(args)
