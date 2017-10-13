#!/usr/bin/python3
try:
    import asyncio
except ImportError:
    import trollius as asyncio
    
from autobahn.asyncio.wamp import ApplicationSession,ApplicationRunner

import json

class Ticker(ApplicationSession):

    def onConnect(self):
        self.join(self.config.realm)
        
    @asyncio.coroutine
    def onJoin(self, details):
        # listening for the corresponding message from the "backend"
        # (any session that .publish()es to this topic).
        def onTicker(*msg):
            
            try:
                print(json.dumps(msg))

            except Exception as e:
                print(e)
        
        try: 
            yield from self.subscribe(onTicker, 'ticker')
        except Exception as e:
            print("Could not subscribe to topic:", e)
        
    def onDisconnect(self):
        asyncio.get_event_loop().stop()


if __name__ == '__main__':
    runner = ApplicationRunner(u"wss://api.poloniex.com:443",u"realm1")
    runner.run(Ticker)