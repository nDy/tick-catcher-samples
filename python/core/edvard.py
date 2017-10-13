#!/usr/bin/python3
'''
Created on Jul 28, 2016

@author: ndy
'''

from subprocess import Popen, PIPE
from multiprocessing.dummy import Process as Thread
import json
import psycopg2

from datetime import datetime as dt
import sys

import atexit
import os

class edvard(object):
    '''
    Edvard Munch
    
    Asynchronous PostgreSQL capable Ticker module
    '''
    
    def __init__(self):
        '''
        Initialization
        '''
        
        self.timeout = 0
        
        self.db = self.conncectToDB()
        self.uptables=[]
        
        self.markets = {}
        self.startTicker()
        
        atexit.register(self.closure)
        
    def getTimeout(self):
        '''
        Timeout getter
        '''
        
        return self.timeout
        
    def startTicker(self):
        '''
        Starts ticker
        '''
        
        self.timeout = dt.now()
        
        script_dir = os.path.dirname(__file__)
        
        rel_path = "tickcatcher.py"
        abs_file_path = os.path.join(script_dir,rel_path)
        
        self._tickerP = Popen([sys.executable,abs_file_path], stdout=PIPE, bufsize=1)
        print('TICKER: tickcatcher subprocess started')
        
        self._tickerT = Thread(target=self.tickCatcher);self._tickerT.daemon = True
        self._tickerT.start()
        print('TICKER: tickCatcher thread started')
    
    def stopTicker(self):
        '''
        Stops ticker
        '''
        self._tickerP.terminate();self._tickerP.kill()
        print('TICKER: Ticker subprocess stopped')
        
        self._tickerT.join()
        print('TICKER: Ticker thread joined')
    
    def clearDB(self):
        '''
        Drops tables created for every pair that sent a tick
        '''
        
        for i in self.uptables:
            self.clearTable(i)
        self.db.close()       
    
    def closure(self):
        '''
        Clears everything when ed is closed
        '''
        
        self.stopTicker()
        #self.clearDB()
        
    def rewind(self):
        '''
        Reboots ticker
        '''
        
        self.stopTicker()
        print("TICKER: Ticker subprocess timeout")
        print("TICKER: Rebooting ticker")
        self.startTicker()
        
    def conncectToDB(self):
        '''
        Open connection to database
        '''
        
        try:
            return psycopg2.connect("dbname='ouroboros' user='postgres' host='localhost' password='150553'")
        except:
            print ("I am unable to connect to the database")
            
    def insertToDB(self,pair,tick,ask,bid):
        '''
        Inserts tick, ask bid to pair table
        '''
        
        try:
            self.timeout = dt.now()
            cur = self.db.cursor()
            aux = "INSERT INTO "
            aux+=(pair.lower())
            aux+=("(ts, tick, ask, bid) VALUES(now(), ")
            aux+=(str(tick))
            aux+=(", ")
            aux+=(str(ask))
            aux+=(", ")
            aux+=(str(bid))
            aux+=(")")
            
            cur.execute(aux)
            
            # Make the changes to the database persistant
            self.db.commit()
        except:
            print ("Query", aux)
            print ("Error InsertToDB", sys.exc_info()[0])
        
    def createTable(self,pair):
        '''
        Creates table for new currency pair
        '''
        
        try:
            cur = self.db.cursor()
            aux = "CREATE TABLE "
            aux+=(pair.lower())
            aux+=("(ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, tick REAL, ask REAL, bid REAL, PRIMARY KEY(ts))")
            cur.execute(aux)
            
            # Make the changes to the database persistant
            self.db.commit()
        except psycopg2.Error as e:
            print ("Error CreateTable", sys.exc_info()[0])
            print (e.pgerror)
            print (e.diag.message_detail)
            
    def tableExists(self,pair):
        
        try:
            cur = self.db.cursor()
            cur.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s)", (pair.lower(),))
            
            return cur.fetchone()[0]
        
        except psycopg2.Error as e:
            print ("Error tableExists", sys.exc_info()[0])
            print (e.pgerror)
            print (e.diag.message_detail)
            
        return True
        
    def clearTable(self,pair):
        '''
        Drops table for currency pair
        '''

        try:
            cur = self.db.cursor()
            aux = "DROP TABLE "
            aux+=(pair.lower())
            
            cur.execute(aux)
            
            self.db.commit()
        except:
            print ("Error ClearTable", sys.exc_info()[0])
    
    def tickCatcher(self):
        '''
        tickCatcher thread, Creates tables if needed, appends ticks to tables
        '''
        
        with self._tickerP.stdout:
            for line in iter(self._tickerP.stdout.readline, b''):
                try:
                    tick = json.loads(line.decode('utf-8'))
                    self.markets[tick[0]] = {
                            'last':tick[1], 
                            'lowestAsk':tick[2], 
                            'highestBid':tick[3], 
                            'percentChange':tick[4], 
                            'baseVolume':tick[5], 
                            'quoteVolume':tick[6], 
                            'isFrozen':tick[7], 
                            'high24hr':tick[8], 
                            'low24hr':tick[9],
                            }
                    if(not(tick[0] in self.uptables)):
                        if not self.tableExists(tick[0]):
                            self.createTable(tick[0])
                        self.uptables.append(tick[0])
                    self.insertToDB(tick[0], tick[1], tick[2], tick[3])
                except Exception as e:
                    print("Handling exception:")
                    print(e)