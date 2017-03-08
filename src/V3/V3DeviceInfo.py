#!/usr/anaconda2/bin/python
# -*- coding: utf-8 -*-
from configInfo import *
import multiprocessing
import sys
sys.path.append("/home/ct_fota/YangShuxuan")
from customPyPackageYSX.FotaEventSYNC import RealTimeEvents	
if __name__=="__main__":

	
	
##########################################fota 5.0###############################################################################
	ps = [ multiprocessing.Process(target = RealTimeEvents(t,"v3device",fota3DataBase,v3DeviceNeedFields,fota3Schema).loopRun) for t in allFota3DeviceTable]
########################################## region ###############################################################################
	ps += [ multiprocessing.Process(target = RealTimeEvents(t,"v3region",fota3DataBase).loopRun) for t in allFota3RegionTable]
	for p in ps:p.start()
	for p in ps:
		p.join()
	print "Game Over"
