'''
Created on Jun 17, 2011

@author: mariano
'''

from multiprocessing import Process
import os

from PIL import Image # Require PIL module.
import imghdr


class ThumbnailGenerator(object):
    
    def __init__(self, inQueue, outQueue, errQueue, proc_count=5):
        self.inQueue = inQueue
        self.outQueue = outQueue
        self.errQueue = errQueue
        self.processes = []
        
        self.conf = {}
        
        self.conf['olx'] = {
            'large' : (625, 625),
            'medium': (180, 180),
            'small' : ( 90,  90),
        }
        
        
        for i in range(proc_count):
            
            _args=(i, inQueue, outQueue, errQueue)
            p = Process(target=self.create_thumbnail, args=_args)
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
    
    def mk_thumbnail(self, imgFile, type, geom, target_path):
        
        if not os.path.exists(target_path):
            os.makedirs(target_path)
        
        fileName = os.path.split(imgFile)[-1]
        
        img = Image.open(imgFile)
        img.thumbnail(geom)

        targetFile = '%s_%s' %(type, fileName)
        destination = os.path.join(target_path, targetFile)
        img.save(destination)
        
        return destination
    
    def on_success(self, ret):
        payload = ret['payload']
        
        payload['fullImageFilename'] = ret['large']
        payload['thumbImageFilename'] = ret['medium']
        payload['smallImageFilename'] = ret['small']
        payload['status'] = 'SUCCESS'
        self.outQueue.put(payload)
    
    def create_thumbnail(self, i, downloadQueue, outQueue, errQueue):

        while True:
            
            queueItem = downloadQueue.get()
            if not queueItem:
                break
            
            oriFileName = queueItem['imgPath']
            type = imghdr.what(oriFileName)
            
            imgFile = oriFileName+'.'+type
            os.rename(oriFileName, imgFile)
            
            trgPath = os.path.join(queueItem['payload']['imgPath'], queueItem['payload']['subfolder']) 
            
            ret = {}
            ret['payload'] = queueItem['payload']
            
            try:
                for key in self.conf['olx'].keys():
                    ret[key] = self.mk_thumbnail( imgFile, key, self.conf['olx'][key], trgPath )
            except OSError:
                self.send_error( 'Unable to create directory', ret)
                continue
                
            self.on_success(ret)