'''
Created on Jun 16, 2011

@author: Mariano Videla
'''

from multiprocessing import Queue

class IMQueue(object):
    
    def __init__(self):
        self.queue = Queue()
        self.req_count = 0
        self.closed = False
        
    def put(self, item):
        self.queue.put( item )
        
        
    def get(self):
        
        if self.closed:
            return None
        
        ret = self.queue.get()

        self.req_count = self.req_count-1
        
        return ret
    
    def close(self, process_count=100):
        
        self.closed = True
        for i in range(process_count):
            self.queue.put(None)
        
        