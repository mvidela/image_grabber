'''
Created on Jun 17, 2011

@author: mariano
'''

'''
Created on Jun 17, 2011

@author: Mariano Videla
'''

from httplib import HTTPConnection
from multiprocessing import Process
from urlparse import urlparse
import os

class CustomConnection(HTTPConnection):
    def __init__(self, host, time_out):
            HTTPConnection.__init__(self, host, timeout=time_out)

    def connect(self):
            #print '====>opening new connection'
            HTTPConnection.connect(self)


def pretty_size( byte_count ):
    if byte_count < 1024:
        return '%.2f bytes' %byte_count
    
    kcount = byte_count / 1024.0
    
    if kcount < 1024:
        return '%.2f KB' %kcount
    
    return '%.2f MB' %(kcount / 1024.0)



class Grabbler(object):
    
    def __init__(self, inQueue, outQueue, errQueue, proc_count=10, headers={}):
        
        self.inQueue = inQueue
        self.outQueue = outQueue
        self.errQueue = errQueue
        self.processes = []
        self.headers = {}
        self.timeOut = 5
        
        for i in range(proc_count):
            
            _args=(i, inQueue, outQueue, errQueue)
            p = Process(target=self.download_image, args=_args)
            self.processes.append( p )
            p.start()
        
    def join(self):
        
        if self.inQueue:
            self.inQueue.close()
            
            for p in self.processes:
                self.inQueue.put(None)
        
        for p in self.processes:
            p.join()
    
    def terminate(self):
        
        for p in self.processes:
            p.terminate()
            
    def send_error(self, msg, item):
        payload = item['payload']
        
        payload['status'] = 'ERROR'
        payload['reason'] = msg
        
        self.errQueue.put(payload)
    
    
    def download_success(self, queueItem):
        req = {}
        
        req['imgPath'] = queueItem['trgPath']
        req['payload'] = queueItem['payload']
        
        self.outQueue.put(req)
            
    def download_image(self, id, downloadQueue, outQueue, errQueue):

        conn = None    
        current_host = None
        
        while True:
            
            queueItem = downloadQueue.get()
            if queueItem is None:
                break
            
            url = queueItem['url']
            
            url_parts = urlparse(url)
            
            host = url_parts.hostname
            if host is None:
                msg = 'Invalid host name [%s]'% host
                self.send_error(msg, queueItem)
                continue
            
            if current_host != host:
                if conn is not None:
                    conn.close()
                
                #current_host = None;
                conn = None
    
            if conn is None:
                #print '===>Creating new connection %s vs %s ' %(current_host, host)
                conn = CustomConnection(host, self.timeOut)
                current_host = host
            
            conn.request('GET', url, None, self.headers)
            try:
                response = conn.getresponse()
                length = response.length
                
                f = open(queueItem['trgPath'], 'wb')
                f.write( response.read() )
                f.close()
                
                queueItem['payload']['download_status'] = response.status
                queueItem['payload']['download_length'] = length
                
                self.download_success(queueItem)
            
            except:
                self.send_error('Unexpected error', queueItem)
                conn = None
                current_host = None
                
        #print 'Download process finished'
        