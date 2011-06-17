'''
Created on Jun 17, 2011

@author: mariano
'''

import os

from PIL import Image # Require PIL module.
import imghdr

from process import Process


class ThumbnailGenerator(Process):
    
    def __init__(self, outQueue, errQueue, proc_count=5):

        self.outQueue = outQueue
        self.errQueue = errQueue
        
        self.conf = {}
        self.conf['olx'] = {
            'large' : (625, 625),
            'medium': (180, 180),
            'small' : ( 90,  90),
        }
        
        Process.__init__(self, proc_count)
        
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
    
    
    def process(self, queueItem):

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
            return self.send_error( 'Unable to create directory', ret)
            
            
        self.on_success(ret)
