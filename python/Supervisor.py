#!/usr/bin/python3
'''
Created on Sep 13, 2016

@author: ndy
'''
from core.edvard import edvard

from datetime import datetime as dt
from datetime import timedelta as delta

if __name__ == '__main__':
    specialED = edvard()
    
    while (True):
        if(dt.now()> specialED.getTimeout()+delta(seconds = 60)):
            specialED.rewind()