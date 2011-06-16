'''
Created on Jun 16, 2011

@author: Mariano Videla
'''

from multiprocessing import Queue, Process

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
        

class ProcessGroup(object):
    
    def __init__(self, inQueue=None):
        self.processes = []
        self.inQueue = inQueue
        
    def start_process(self, function, params, count=1):
        for i in range(count):
            t = (i,)+params
            p = Process(target=function, args=t)
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
            
    