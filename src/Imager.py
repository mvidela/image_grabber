'''
Created on Jun 15, 2011

This is a simple script that help kfeeds to download
images and to generate thumbnails.



@author: Mariano Videla
'''


from multiprocessing import Process
import sys, os
import json
import hashlib

from grabbler import Grabbler
from thumbnail import ThumbnailGenerator
from IMQueue import IMQueue



def write_stdout(queue):
    '''Reads a input from the queue an writes it to the stout'''
    
    #print 'Writer process started'
    while True:
        response = queue.get()
        if response is None:
            break
        
        sys.stdout.write(json.dumps(response))
        sys.stdout.write('\n')
        sys.stdout.flush()


def parse_request(msg):
    req = {}
    
    try:
        dict = json.loads(msg)
        
    except ValueError:
        dict = {}
        dict['url'] = msg
        dict['tmpImgPath'] = '~/tmp2/images'
        dict['olxId'] = '-1'
    
    url = dict['url']
    base_dir = dict['tmpImgPath']
    olx_id = dict['olxId']
    
    
    req['url']=url 
    req['trgPath'] = get_image_path(base_dir, 'f', olx_id, url)
    req['payload'] = dict
    
    return req

    
def get_image_path(baseDir, prefix, olx_id, url, extension='none'):
    
    if not os.path.exists(baseDir):
        os.makedirs(baseDir)
    
    crc = get_hash(url)
    fname = ('%s_%s_%s' % (prefix, olx_id, crc))
    
    return os.path.join( baseDir, fname )

def get_hash(content):
    
    shaobj = hashlib.sha1()
    shaobj.update(content)
    
    return shaobj.hexdigest()

def enqueue_items(queue):
    
    while not sys.stdin.closed:
        line = sys.stdin.readline()
        if line:        
            line = line.strip('\n')
            
            req = parse_request(line)
            
            queue.put(req)
        else:
            break

    #print 'Left the loop'
            

if __name__ == '__main__':
    
    notifyQueue = IMQueue()
    
    thumbs = ThumbnailGenerator(notifyQueue, notifyQueue, 5)
    grabbler = Grabbler(thumbs.get_queue(), notifyQueue, 20)
    
    producer = Process(target=write_stdout, args=(notifyQueue,))
    producer.start()

    try:    
        enqueue_items(grabbler.get_queue())
        
    except KeyboardInterrupt:
        #kill all involved processes
        for p in (grabbler, thumbs, producer):
            p.terminate()
            p.join()
        
        sys.exit(-1)
    
    #join the process
    grabbler.join()
    thumbs.join()
 
    notifyQueue.close(1)
    producer.join() 
