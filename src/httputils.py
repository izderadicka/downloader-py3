

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
from time import sleep, time
import functools 
import os.path, os
from http.client import BadStatusLine, IncompleteRead


class ResponseProxy():
    def __init__(self, req):
        self._info=req.info()
        self._data=BytesIO(req.read())
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
        ct_hdrs = http_message.get("content-type")
        if ct_hdrs in ('text/html', 'text/xhtml'):
            r=ResponseProxy(response)
           
            try:
                pg=BeautifulSoup(r, 'lxml')
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
    



#Single threaded client 
#TODO: Can leverage multiple threads?
class HTTPClient:
    global_cookies_jar=CookieJar()
    def __init__(self, proxy=None, no_proxy=False, repeats_on_failure=5,
                 average_wait_between_requests=0, max_wait_between_requests=0):
        self.repeats_on_failure=repeats_on_failure
        self.proxy=proxy
        self.no_proxy=no_proxy
        self.opener=self.build_opener()
        self.last_request_time=0
        self.average_wait_between_requests=average_wait_between_requests
        self.max_wait_between_requests=max_wait_between_requests
    
    class Error(BaseException): pass
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
            sleep(pause)
        self.last_request_time=time()
        
    def open_url(self, url, post_args=None, timeout=30, resume=None, refer_url=None):
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
                res=self.opener.open(req, timeout=timeout)
                break
            except (IOError, urllib2.HTTPError, BadStatusLine, IncompleteRead) as e:
                pause=self._get_random_interval(self.average_wait_between_requests, self.max_wait_between_requests)
                logging.warn('IO or HTTPError (%s) while trying to get url %s, will retry in %f secs' % (str(e),url, pause))
                retries-=1
                sleep(pause)
                self.last_request_time=time()
        if not res:
            raise HTTPClient.Error('Cannot load resource %s' % url)
        return res
    
    def save_file(self, url, filename, post_args=None, timeout=None, resume=True, refer_url=None): 
        res=self.open_url(url, post_args, timeout, resume, refer_url)
        p,f=os.path.split(filename)
        if not os.path.exists(p):
            os.makedirs(p)
        with open(filename,'wb') as f:
            
            while True:
                r=res.read(1048576)
                if not r: break
                f.write(r)
           
        
    def load_page(self,url, post_args=None, timeout=None):
        
        res=self.open_url(url, post_args, timeout)        
            
        header=res.info()
        data=res.read()
        if header.get('Content-Encoding')=='gzip':
            tmp_stream=gzip.GzipFile(fileobj=BytesIO(data))
            data=tmp_stream.read()
            
        elif header.get('Content-Encoding')=='deflate':
            data = zlib.decompress(data)
            
        logging.debug('Loaded page from url %s' % url)
        pg=BeautifulSoup(data, 'lxml')
        return pg


class TestSpider():
    """ Mixin for UnitTests"""
    def __init__(self, file_name):
        f=open(file_name)
        self.page=BeautifulSoup(f.read(), 'lxml')
        self.links_generator=self.next_link(self.page)
        self.stopping=False
    def next_page(self):
        return False

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
                url= restored_url or url
       
        self.page=client.load_page(url)
        if self.require_login(self.page):
            self.login()
            self.page=client.load_page(url)
        self.curr_url=url
        self.links_generator=self.next_link(self.page)
        self.stopping=False
    
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
        if not self.links_generator or self.stopping:
            raise StopIteration
        try:
            return self.postprocess_link(*next(self.links_generator))
        except StopIteration:
            if self.next_page():
                return self.__next__()
            else:
                raise StopIteration
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
        
        