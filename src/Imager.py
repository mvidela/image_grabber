'''
Created on Jun 15, 2011

This is a simple script that help kfeeds to download
images and to generate thumbnails.



@author: Mariano Videla
'''

from PIL import Image # Require PIL module.
from httplib import HTTPConnection
from multiprocessing import Process, Queue
from urlparse import urlparse
import sys
import os.path

from IMQueue import IMQueue, ProcessGroup

headers = {'Content-Length': '0',
           'User-Agent' : 'Mozilla/5.0 (X11; Linux i686) AppleWebKit/534.30 (KHTML, like Gecko) Chrome/12.0.742.100 Safari/534.30'
}

time_out = 5



class CustomConnection(HTTPConnection):
    def __init__(self, host):
            HTTPConnection.__init__(self, host, timeout=time_out)

    def connect(self):
            print '====>opening new connection'
            HTTPConnection.connect(self)


def pretty_size( byte_count ):
    if byte_count < 1024:
        return '%.2f bytes' %byte_count
    
    kcount = byte_count / 1024.0
    
    if kcount < 1024:
        return '%.2f KB' %kcount
    
    return '%.2f MB' %(kcount / 1024.0)

def download_image(id, downloadQueue, outQueue):

    conn = None    
    current_host = None
    
    while True:
        
        queueObj = downloadQueue.get()
        if queueObj is None:
            break
        
        url, targetFile = queueObj
        url_parts = urlparse(url)
        
        host = url_parts.hostname
        if host is None:
            continue
        
        if current_host != host:
            if conn is not None:
                conn.close()
            
            #current_host = None;
            conn = None

        if conn is None:
            print '===>Creating new connection %s vs %s ' %(current_host, host)
            conn = CustomConnection(host)
            current_host = host
        
        conn.request('GET', url, None, headers)
        try:
            response = conn.getresponse()
            outQueue.put('P%d - %s - %d - Length: %s' % (id, url, response.status, pretty_size(response.length) ) )
        
            response.read()
        except:
            print 'Error with %s.  Reseting connection' %url
            conn = None
            current_host = None
            
    print 'Download process finished'
    
def create_thumbnail(i, downloadQueue, outQueue):

    while True:
        
        imgFile, width, height = downloadQueue.get()
        img = Image.open(imgFile)
        img.thumbnail((width, height))
         
        img.save(imgFile + ".thumb")

         


def write_stdout(queue):
    '''Reads a input from the queue an writes it to the stout'''
    
    #print 'Writer process started'
    while True:
        response = queue.get()
        if response is None:
            break
        sys.stdout.write('Got response: %s\n' % response)
        sys.stdout.flush()
    
    #print 'Writer process ended'
    
def enqueue_items():
    
    while not sys.stdin.closed:
        line = sys.stdin.readline()
        if line:        
            line = line.strip('\n')
            downloadQueue.put([line, '/tmp/file0'])
        else:
            break

    print 'Left the loop'
            

if __name__ == '__main__':
    
    downloadQueue = IMQueue()
    thumbQueue = IMQueue()
    notifyQueue = IMQueue()
    
    download_process_count = 20
    if len(sys.argv) > 1:
        download_process_count = int(sys.argv[1], 10)
        
    thumb_process_count = 5
    if len(sys.argv) > 2:
        thumb_process_count = int(sys.argv[2], 5)
    
    downloaders = []
    
    downloadGroup = ProcessGroup(downloadQueue)
    downloadGroup.start_process( target=download_image, 
                                (downloadQueue, thumbQueue), 
                                download_process_count )
    
    thumbnailGroup = ProcessGroup(thumbQueue)
    downloadGroup.start_process( target=create_thumbnail, 
                                (thumbQueue, notifyQueue), 
                                thumb_process_count )
    
    for i in range(download_process_count):
        downloaders.append(Process())
        downloaders[i].start()
        
    for i in range(thumb_process_count):
        downloaders.append(Process(target=create_thumbnail, args=(i, thumbQueue, notifyQueue)))
        downloaders[i].start()
    
    
    producer = Process(target=write_stdout, args=(notifyQueue,))
    producer.start()

    try:    
        
        enqueue_items()
        
    except KeyboardInterrupt:
        #kill all involved processes
        downloadGroup.kill()
        thumbnailGroup.kill()
        producer.terminate()
        
        sys.exit(-1)
    
    downloadQueue.close(download_process_count)

    for consumer in downloaders:
        consumer.join() 
    
    notifyQueue.close(1)
    producer.join() 
