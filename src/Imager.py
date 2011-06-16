'''
Created on Jun 15, 2011

This is a simple script that help kfeeds to download
images and to generate thumbnails.



@author: Mariano Videla
'''

import sys
import time
from multiprocessing import Process, Queue
from httplib import HTTPConnection
from urlparse import urlparse
from PIL import Image # Require PIL module.

from IMQueue import IMQueue

headers = "'User-Agent' : 'Mozilla/5.0 (X11; Linux i686) AppleWebKit/534.30 (KHTML, like Gecko) Chrome/12.0.742.100 Safari/534.30'"
#headers = ''



class CustomConnection(HTTPConnection):
    def __init__(self, host):
            HTTPConnection.__init__(self, host)

    def connect(self):
            print '====>opening new connection'
            HTTPConnection.connect(self)


def download_image(id, inQueue, outQueue):

    conn = None    
    current_host = None
    
    while True:
        
        queueObj = inQueue.get()
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
        
        conn.request('GET', url, headers)
        try:
            response = conn.getresponse()

            outQueue.put('P%d - %s - %d' % (id, url, response.status))
        
            response.read()
        except:
            print 'Error with %s.  Reseting connection' %url
            conn = None
            current_host = None
            
    print 'Download process finished'
    

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

if __name__ == '__main__':
    
    inQueue = IMQueue()
    outQueue = IMQueue()
    
    process_count = 20
    if len(sys.argv) > 1:
        process_count = int(sys.argv[1], 10)
    
    consumers = []
    
    for i in range(process_count):
        consumers.append(Process(target=download_image, args=(i, inQueue, outQueue)))
        consumers[i].start()
    
    
    producer = Process(target=write_stdout, args=(outQueue,))
    producer.start()

    try:    
        while not sys.stdin.closed:
            line = sys.stdin.readline()
            if line:        
                line = line.strip('\n')
                inQueue.put([line, '/tmp/file0'])
            else:
                break
    
        print 'Left the loop'
            
        inQueue.close(process_count)
    
        for consumer in consumers:
            consumer.join() 
        
        outQueue.close(1)
        producer.join()
    

    except KeyboardInterrupt:
        for p in consumers:
            p.terminate()
        consumer.terminate
        
