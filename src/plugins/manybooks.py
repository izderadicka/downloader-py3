'''
Created on May 11, 2012

@author: ivan
'''


import logging
import re
import os.path
from httputils import LinksSpider
from optparse import Option


# Some basic constants for particular site
# client settings for links parsing
REPEATS=5 # how many times to retry after HTTP or connection error
MEAN_WAIT=6 # average time to wait before next request - in seconds
MAX_WAIT=18 # maximum time to wait - if random wait if greater it is reduced to this maximum

# clients settings for files downloading
# same meaning as above, but for files downloading
REPEATS2=5
MEAN_WAIT2=1
MAX_WAIT2=2

# maximum number of files links in queue, when reached parsing of new links
# blocks, until a link is downloaded and can push to queue again 
MAX_QUEUE_SIZE=50 #0 means unlimited
DOWN_THREADS=2 # number of threads for download tasks

#Base URL to use for relative links processing
BASE_URL = 'http://manybooks.net'
# URL to start with
START_URL='http://manybooks.net/language.php?code=en&s=1'

# Set by program - a directory where downloaded files should be stored
BASE_DIR=''

#command line options for this plugin
# available the from options variable as options.name
OPTIONS=[Option('--store_path', type='string', help='Template for path where file is stored. '+
                'following keys available {title} {author} {id}'),
        ]
options=None # will be filled with parsed options 

#Create MySpider class - to get all links form one page and also to get link to next page
class MySpider(LinksSpider):
    
    #Assure that you are logged into site and any cookies set
    # self.client is available HTTPClient to load any page needed
    def login(self):
        pg=self.client.load_page(BASE_URL)
        
    # Do we require to login
    # page - is parsed page as BeutifulSoup object
    # return true is yes
    def require_login(self, page):
        return not page.find('span', 'googlenav') is None
    
    # function to parse available links on a page
    # page - is parsed page as BeutifulSoup object
    # must be generator or return iterator returning (link, metadata) tuple       
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
                    # Is generator !
                    yield BASE_URL+link_url, metadata
                except:
                    logging.exception('error parsing book data')
                    continue
                    
       
    # Parse page to find URL for next page
    # page - is parsed page as BeutifulSoup object
    # returns url or false if there is no further page
    def next_page_url(self,page):
        try:
            pager=page.find('span', 'googlenav')
            current_page=pager.find('strong', recursive=False)
            next_url=current_page.find_next_sibling('a')
            
            if next_url and next_url.get('href'): 
                next_url=str(next_url['href'])
                # for testing can stop on page 5
                #if next_url and next_url.find('s=6')>=0: return
                logging.debug('Has next page %s' % next_url)
                return BASE_URL+next_url
        except:
            logging.exception('Error while parsing page for next page')
    
    # Postprocess links found on page and get final link and metadata
    # can get another page   via self.client and do some more parsing
    # or anything else needed
    # link, metadata is tuple returned by next_link    
    # must return (link, metadata) tuple 
    # if for any reason link should not be downloaded method should  raise SkipLink
    def postprocess_link(self, link, metadata):
        if not metadata.get( 'author'):
            metadata['author']='Unknown Author'
        if not metadata.get( 'title'):
            metadata['title']='Unknown Title'
        return link, metadata

       
def _meta_to_filename(base_dir,metadata, ext):
    templ="{author}/{title}/{author} - {title}[{id}]"
    if options.store_path:
        templ=options.store_path
    while templ.startswith(os.sep):
        templ=templ[1:]
    p=templ.format(**metadata) +ext
    return os.path.join(base_dir,p)

# function to save  file from found link
# client is   HTTPClient object to load and save file
# link is a link to file
# metadata is a dictionary of metadata as returned by MySpider.next_link
# base_dir is directory to save file
# context - get access to worker running this function 
#           context.sleeper.sleep - can be used to sleep for x seconds
def save_file(client,link, metadata, base_dir, context=None):
    filename=_meta_to_filename(base_dir, metadata, '.epub')
    data={'book':'1:epub:.epub:epub', 'tid': metadata['id']}
    client.save_file(BASE_URL+'/_scripts/send.php', filename, data,refer_url=link)
    meta=repr(metadata)
    with open(filename+'.meta', 'w') as f: 
        f.write(meta)  
    logging.debug('Saved file %s'% filename)
    


