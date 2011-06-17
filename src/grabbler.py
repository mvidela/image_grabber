'''
Created on Jun 17, 2011

@author: mariano
'''

'''
Created on Jun 17, 2011

@author: Mariano Videla
'''

from httplib import HTTPConnection
from process import Process
from urlparse import urlparse


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



class Grabbler(Process):
    
    def __init__(self, outQueue, errQueue, proc_count=10, headers={}):

        self.outQueue = outQueue
        self.errQueue = errQueue
        self.headers = {}
        self.timeOut = 5
        
        Process.__init__(self, proc_count)
        
    def _loop(self):
        
        conn = None    
        current_host = None
        
        while True:
            queueItem = self.inQueue.get()
            if not queueItem:
                break
            
            self.download_image(conn, current_host, queueItem)
        
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
            
    def download_image(self, current_connection, current_host, queueItem):

        url = queueItem['url']
        url_parts = urlparse(url)

        host = url_parts.hostname
        
        if host is None:
            msg = 'Invalid host name [%s]'% host
            return self.send_error(msg, queueItem)
            
            
        if current_host != host and current_connection is not None:
            current_connection.close()
            current_connection = None
    
        if current_connection is None:
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
                
