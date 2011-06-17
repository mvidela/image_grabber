'''
Created on Jun 17, 2011

@author: mariano
'''
import multiprocessing

from IMQueue import IMQueue


class Process(object):
    
    def __init__(self, process_count, inQueue=None):
        
        if not inQueue:
            inQueue = IMQueue()
        self.inQueue = inQueue
        self.processes = []
        
        for i in range(process_count):
            p = multiprocessing.Process(target=self._loop)
            self.processes.append(p)
            p.start()
            
    def get_queue(self):
        
        return self.inQueue

    def _loop(self):
        
        while True:
            queueItem = self.inQueue.get()
            if not queueItem:
                break
            
            self.process(queueItem)
    
    def process(self, queueItem):
        print queueItem


    def submit(self, item):
        self.inQueue.put(item)
        

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
    