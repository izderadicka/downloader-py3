'''
Created on May 11, 2012

@author: ivan
'''


import logging
import re
import os.path
from httputils import LinksSpider


# Some basic constants for particular site
# client settings for links parsing
REPEATS=5
MEAN_WAIT=6
MAX_WAIT=18
# clients settings for files downloading
REPEATS2=5
MEAN_WAIT2=1
MAX_WAIT2=2
#
MAX_QUEUE_SIZE=50 #0 means unlimited
DOWN_THREADS=2 # number of threads for download tasks
#
BASE_URL = 'http://manybooks.net'
START_URL='http://manybooks.net/language.php?code=en&s=1'

BASE_DIR=''

#Create MySpider class - to get all links form one page and also to get link to another page
class MySpider(LinksSpider):
    
    def login(self):
        pg=self.client.load_page(BASE_URL)
        
    
    def require_login(self, page):
        return not page.find('span', 'googlenav') is None
    #must be generator or return iterator returning link, metadata       
    def next_link(self, page):
        table=page.find('div', 'table')
        if table:
            cells=table.find_all('div', 'grid_5')
            for cell in cells:
                try:
                    metadata={}
                    metadata['author']=str(list(cell.strings)[-1]).strip()
                    link=cell.find('a')
                    if link: 
                        metadata['title']=str(link.string).strip()
                        link_url=link['href']
                        m=re.match(r'/titles/(.*).html', link_url)
                        if m:
                            metadata['id']=m.group(1)
                        if not link_url:
                            continue
                    else:
                        continue
                    sub=cell.find('em')
                    if sub:
                        metadata['subtitle']=str(sub.string).strip()
                    yield BASE_URL+link_url, metadata
                except:
                    logging.exception('error parsing book data')
                    continue
                    
       
    
    def next_page_url(self,page):
        try:
            pager=page.find('span', 'googlenav')
            current_page=pager.find('strong', recursive=False)
            next_url=current_page.find_next_sibling('a')
            
            if next_url and next_url.get('href'): 
                next_url=str(next_url['href'])
                if next_url and next_url.find('s=6')>=0: return
                logging.debug('Has next page %s' % next_url)
                return BASE_URL+next_url
        except:
            logging.exception('Error while parsing page for next page')
        
def meta_to_filename(base_dir,metadata, ext):
    p=os.path.join(base_dir, metadata.get('author','None'), metadata.get('title'), 
                   metadata.get('author','None') + ' - ' +metadata.get('title') + '['
                   +metadata.get('id', 'None')+']'+ext)
    return p

#implement function to save  file from found link   
def save_file(client,link, metadata, base_dir):
    filename=meta_to_filename(base_dir, metadata, '.epub')
    data={'book':'1:epub:.epub:epub', 'tid': metadata['id']}
    client.save_file(BASE_URL+'/_scripts/send.php', filename, data,refer_url=link)
    meta=repr(metadata)
    with open(filename+'.meta', 'w') as f: 
        f.write(meta)  
    logging.debug('Saved file %s'% filename)
    


