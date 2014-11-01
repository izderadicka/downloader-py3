

from bs4 import BeautifulSoup
from urllib.request import  BaseHandler
import urllib.request as urllib2
from urllib.parse import urlencode
import logging
import zlib
import gzip
from io import BytesIO
import random
from http.cookiejar import CookieJar, Cookie
from time import time
import functools 
import os.path, os
from http.client import BadStatusLine, IncompleteRead
from persistent_queue import Sleeper, Interrupted
import socket
import signal

PARSER='lxml'#'html5lib'#'html.parser'#'lxml'
# Will this help with read timeout?
socket.setdefaulttimeout(120)

class Timeout():
    """Timeout class using ALARM signal"""
    class Timeout(Exception): pass
    
    def __init__(self, sec):
        self.sec = sec
    
    def __enter__(self):
        signal.signal(signal.SIGALRM, self.raise_timeout)
        signal.alarm(self.sec)
    
    def __exit__(self, *args):
        signal.alarm(0) # disable alarm
    
    def raise_timeout(self, *args):
        raise Timeout.Timeout()

class ResponseProxy():
    def __init__(self, req):
        self._info=req.info()
        
        if self._info.get('Content-Encoding')=='gzip' or  \
        self._info.get('Content-Encoding')=='deflate':
            data=decode_data(req);
            del self._info['Content-Encoding']
        else:
            data=req.read()
        self._data=BytesIO(data)
        req.close()
        self.url=req.geturl()
        self.code, self.msg= req.code, req.msg
        
    def read(self, *args):
        return self._data.read(*args)
    
    def readline(self):
        return self._data.readline()
    
    def info(self):
        return self._info
    
    def geturl(self):
        return self.url
    
    def close(self):
        self._data.close()
        
    def seek(self, idx):
        
        self._data.seek(idx)
        

class HTTPEquivProcessor(BaseHandler):
    """Append META HTTP-EQUIV headers to regular HTTP headers."""


    
    def http_response(self, request, response):
        
        http_message = response.info()
        r=response
        ct_hdrs = http_message.get("Content-Type")
        if not ct_hdrs:
            return r
        ct_hdrs = ct_hdrs.split(';')[0]
        if ct_hdrs in ('text/html', 'text/xhtml'):
            r=ResponseProxy(response)
           
            try:
                pg=BeautifulSoup(r, PARSER)
                if pg.head:
                    headers =pg.head.find_all('meta', {'http-equiv':True})
                    for h in headers:
                        http_message[h['http-equiv'].capitalize()]=str(h['content'])
                        
            except BaseException as e:
                logging.error('Syntax error while parsing : %s' % e)
           
            finally:
                r.seek(0)
        return r

    https_response = http_response
    
def decode_data(res):
    header=res.info()
    data=res.read()
    if header.get('Content-Encoding')=='gzip':
        tmp_stream=gzip.GzipFile(fileobj=BytesIO(data))
        data=tmp_stream.read()
        
    elif header.get('Content-Encoding')=='deflate':
        data = zlib.decompress(data)
    return data


#Single threaded client 
#TODO: Can leverage multiple threads?
class HTTPClient:
    global_cookies_jar=CookieJar()
    def __init__(self, proxy=None, no_proxy=False, repeats_on_failure=5,
                 average_wait_between_requests=0, max_wait_between_requests=0, timeout=60):
        self.repeats_on_failure=repeats_on_failure
        self.proxy=proxy
        self.no_proxy=no_proxy
        self.opener=self.build_opener()
        self.last_request_time=0
        self.average_wait_between_requests=average_wait_between_requests
        self.max_wait_between_requests=max_wait_between_requests
        self.sleeper=Sleeper()
        self.timeout=timeout
    
    class Error(Exception): pass
    
    class AdditionalHeaders(urllib2.BaseHandler):
        def http_request(self, req):
            req.add_header('User-Agent', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:12.0) Gecko/20100101 Firefox/12.0')
            req.add_header('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8')
            req.add_header('Accept-Language', 'en-us,en;q=0.5')
            req.add_header('Accept-Encoding', 'gzip, deflate')
            return req
        https_request=http_request
    
    def build_opener(self):    
        handlers=[HTTPClient.AdditionalHeaders, urllib2.HTTPHandler, ]
        if self.no_proxy:
            handlers.append(urllib2.ProxyHandler({}) )
        elif self.proxy:
            handlers.append(urllib2.ProxyHandler({'http':self.proxy, 'https':self.proxy}))        
        handlers.extend([ HTTPEquivProcessor, urllib2.HTTPCookieProcessor(HTTPClient.global_cookies_jar)])
        opener=urllib2.build_opener(*handlers)
        return opener
    def _get_random_interval(self, mid, max):
        while True:
            r=random.gauss(mid, (max -mid)/3)
            if r>=0 and r <= max:
                return r
    def _delay(self, url):
        if not self.average_wait_between_requests or not self.average_wait_between_requests:
            return
        pause=self._get_random_interval(self.average_wait_between_requests, self.max_wait_between_requests)
        time_from_last_request=time()-self.last_request_time
        pause=pause-time_from_last_request
        if pause>0:
            logging.debug('Waiting %f seconds to %s'% (pause, url))
            self.sleeper.sleep(pause)
        self.last_request_time=time()
        
    def open_url(self, url, post_args=None, resume=None, refer_url=None):
        if post_args:
            post_args=bytes(urlencode(post_args), 'UTF-8')
        retries=self.repeats_on_failure
        res=None
        req=urllib2.Request(url, post_args)
        if refer_url:
            req.add_header('Referer', refer_url)
        self._delay(url)
        while retries:   
            try:
                #with Timeout(timeout+1) : # sometimes socket timeout is not working
                res=self.opener.open(req, timeout=self.timeout)
                break
            except (IOError, urllib2.HTTPError, BadStatusLine, IncompleteRead, socket.timeout) as e:
                if isinstance(e, urllib2.HTTPError) and hasattr(e,'code') and str(e.code)=='404':
                    raise NotFound('Url %s not found'%url)
                pause=self._get_random_interval(self.average_wait_between_requests, self.max_wait_between_requests)
                logging.warn('IO or HTTPError (%s) while trying to get url %s, will retry in %f secs' % (str(e),url, pause))
                retries-=1
                self.sleeper.sleep(pause)
                self.last_request_time=time()
        if not res:
            raise HTTPClient.Error('Cannot load resource %s' % url)
        return res
    
    def save_file(self, url, filename, post_args=None, resume=True, refer_url=None): 
        res=self.open_url(url, post_args, resume, refer_url)
        p,f=os.path.split(filename)
        if not os.path.exists(p):
            os.makedirs(p)
        with open(filename,'wb') as f:
            
            while True:
                r=res.read(1048576)
                if not r: break
                f.write(r)
           
        
    def load_page(self,url, post_args=None):
        
        res=self.open_url(url, post_args)        
        data=decode_data(res)
            
        logging.debug('Loaded page from url %s' % url)
        pg=BeautifulSoup(data, PARSER)
        return pg
    
    def stop(self):
        self.sleeper.stop()
        
class NotFound(HTTPClient.Error): pass


class TestSpider():
    """ Mixin for UnitTests"""
    def __init__(self, file_name):
        f=open(file_name)
        self.page=BeautifulSoup(f.read(), PARSER)
        self.links_generator=self.next_link(self.page)
        self.stopping=False
    def next_page(self):
        return False

class SkipLink(Exception):
    pass

class LinksSpider:
    def __init__(self, client, url, last_page_file=None):
        self.client=client
       
        self.last_page_file=last_page_file
        if last_page_file and os.access(last_page_file, os.R_OK):
            with open(last_page_file, 'r') as f:
                restored_url=f.read()
                if restored_url=='None': 
                    self.links_generator=None
                    return
                else:
                    restored_url= self.url_restored(restored_url)
                url= restored_url or url
        
        wait=client.max_wait_between_requests
        client.max_wait_between_requests=max(3, round(wait/10.0))
        self.page=client.load_page(url)
        if self.require_login(self.page):
            self.login()
            self.page=client.load_page(url)
        client.max_wait_between_requests=wait
        self.curr_url=url
        self.links_generator=self.next_link(self.page)
        self.stopping=False
        
    def url_restored(self, restored_url):
        """To be overridden if derived class need to modify restored url"""
        return restored_url
    
    def stop(self):
        self.stopping=True
        
    def login(self):
        raise NotImplemented
    
    def require_login(self, page):
        return False
    
    #must be generator or return iterator returning link, metadata       
    def next_link(self, page):
        raise NotImplemented
    
    def next_page_url(self,page):
        raise NotImplemented
        
    def __next__(self):
        
        while True:
            if not self.links_generator or self.stopping:
                raise StopIteration
            try:
                next_link=next(self.links_generator)
                return self.postprocess_link(*next_link)
                break
            except StopIteration:
                if not self.next_page():
                    raise StopIteration
            except SkipLink:
                pass
                
    def postprocess_link(self, link, metadata):
        return link, metadata    
    
    def on_next_page(self, pg_url):
        pass
    
    def _save_url(self,url):
        if self.last_page_file and os.access(os.path.split(self.last_page_file)[0], os.W_OK):
            with open(self.last_page_file, 'w') as f:
                f.write(url)
    def next_page(self):
        
        url=self.next_page_url(self.page)
        if url==self.curr_url:
            url=None
            logging.warn('Return same url for next page as current url: %s'%url)
        self.on_next_page(url)
        if not url:
            #self._save_url('None')
            logging.info('Last page reached on page %s', self.curr_url)
            return False
        
        self.page=self.client.load_page(url)
        if self.require_login(self.page):
            self.login()
            self.page=self.client.load_page(url)
        self.links_generator=self.next_link(self.page)
        self._save_url(url)
        self.curr_url=url
        return self.page
            
    
    #Iterator interface support
    def __iter__(self):
        return self
        
        