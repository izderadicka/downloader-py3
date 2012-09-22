'''
Created on May 16, 2012

@author: ivan
'''
import unittest
from plugins.manybooks import MySpider
from bs4 import  BeautifulSoup
import httputils

class TestSpider( httputils.TestSpider, MySpider):
    pass
    

class Test(unittest.TestCase):


    def setUp(self):
        pass


    def tearDown(self):
        pass


    def testLinks(self):
        spider=TestSpider('../../test_data/manybooks.html')
        links=list(spider)
        for l,m in links: print(l,m)
        self.assertEqual(len(links), 20)
        
    
    def testNextPageURL(self):
        spider=TestSpider('../../test_data/manybooks.html')
        url=spider.next_page_url(spider.page)
        
        self.assertEqual(url, 'http://manybooks.net/language.php?code=en&s=2')
        
    
        
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testLinks']
    unittest.main()