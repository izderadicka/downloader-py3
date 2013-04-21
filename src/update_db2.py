'''
Created on Jul 3, 2012

@author: ivan
'''

import csv
import sys
import dbm.gnu as db
import shelve
import logging
log = logging.getLogger()
logging.basicConfig(level=logging.DEBUG)



def main():
    
    db_file=sys.argv[1]
    
    
    done_db= shelve.Shelf(db.open(db_file, "c")) 
    
    log.info ('db has %d records', len(done_db))
    
    added=0
    try:
        for csv_file in sys.argv[2:]:
            log.info("processing file %s", csv_file)
            reader=csv.DictReader(open(csv_file))
            loaded_files={}
            for row in reader:
                file_name=row['file'].strip()
                if file_name:
                    loaded_files[row['id']]=row
            log.info('csv file has %d valid records ',  len(loaded_files))
                  
                
            for idx in loaded_files:
                if idx not in done_db:
                    done_db[idx]=loaded_files[idx]
                    added+=1
                    
        log.info('Done -  %d added - db now has %d records',  added, len(done_db))        
            
                
    finally:
        done_db.close()
        

if __name__=='__main__':
    main()