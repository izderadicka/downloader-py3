'''
Created on Feb 11, 2014

@author: ivan
'''
import unittest
import sys
from lxml import etree  # @UnresolvedImport
from io import StringIO
import os
import platform

class ParserTarget(object):
    def __init__(self):
        self.rows=0
        self.started=0
        
    def start(self, tag, attrib):
        print("start %s %r" % (tag, dict(attrib)))
        
        try:
        
            if tag=='table' and attrib.get('class') and attrib['class']=='list_table2':
                self.started+=1
                
            if tag=='table' and self.start:
                self.started+=1
                
            if self.started and tag=="tr" and attrib.get('style') and attrib.get('onmouseover'):
                self.rows+=1
                
        except Exception as e:
            print('ERROR', e)
            raise e
            
        
    def end(self, tag):
        print("end %s" % tag)
        if self.started and tag=='table':
            self.started-=1
    def data(self, data):
        print("data %r" % data)
    def comment(self, text):
        print("comment %s" % text)
    def close(self):
        print("close")
        return "closed!"
class Test(unittest.TestCase):

#looks like lxml have problem with libxml2  ver. 2.9.0 this test fails on that particular version
#but work on other versions (previous o 2.9.1)
    def testParser(self):
        
        with open('test1.html', 'rt') as f:
            #load test html
            html=f.read()
            
            # get all trs in table.list_table2 - should be 7 of them
            parser=etree.HTMLParser()
            
            root=etree.parse(StringIO(html), parser)
            t=root.find('.//table[@class="list_table2"]')
            rows=t.findall('tr[@style][@onmouseover]')
            rows_count=len(rows)
            
            #now trying same with target object
            target=ParserTarget()
            parser=etree.HTMLParser(target=target)
            parser.feed(html)
            parser.close()
            
            log=parser.error_log
            
            
            print('ERRORS: ',len(log))
            self.assertTrue(not len(log))
            print ('PYTHON VERSION:', sys.version, 'ON', platform.platform())
            print('LXML VERSION:', etree.LXML_VERSION, 'LIBXML VERSION:', etree.LIBXML_VERSION, ', ', etree.LIBXML_COMPILED_VERSION)
            #should get same number
            self.assertEqual(rows_count, target.rows)


if __name__ == "__main__":
    import sys;sys.argv = ['', 'Test.testParser']
    unittest.main()