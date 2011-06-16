'''
Created on Jun 15, 2011

@author: mariano
'''

import sys
from tornado import ioloop, httpclient


class AsyncDownloader(object):
    
    def __init__(self, slots):
        self._original_slots = slots
        self.slots = slots
        self.loop = ioloop.IOLoop()

    def run(self):
        self.loop.add_callback(self.tick)
        self.loop.start()

    def tick(self):
        while self.slots:
            url = self.get_url()

            if url is None:
                if self.slots == self._original_slots:
                    print 'Quitting'
                    sys.exit()
                print 'Quitting'
                return
            
            request = httpclient.AsyncHTTPClient(io_loop=self.loop)
            print 'Fetching %s' %url
            request.fetch(url, self.on_response)
            self.slots -= 1

    def on_response(self, response):
        print 'Got response'
        print response.error
        self.slots += 1
        self.tick()
        # save response to disk...

    def get_url(self):
        #return sys.stdin.readline()
        return 'http://image.rentals.com/imgr/7c09b6a6f975c27e9d9d1bc7bae3f096/'
    
    

if __name__ == '__main__':
    
    a = AsyncDownloader(10)
    a.run()
        
