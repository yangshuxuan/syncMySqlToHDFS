#!/usr/anaconda2/bin/python
# -*- coding: utf-8 -*-
from configInfo import *
import multiprocessing
import sys
sys.path.append("/home/ct_fota/YangShuxuan")
from customPyPackageYSX.FotaEventSYNC import RealTimeEvents	
if __name__=="__main__":

#(self,deviceInfoTableName,kind,dataBaseInfo,needFields="*",schema=None):
##########################################fota 5.0###############################################################################
	#ps = [ multiprocessing.Process(target = RealTimeEvents(t,"v5device",fota5Schema,fota5DataBase,v5DeviceNeedFields).loopRun) for t in allFota5DeviceTable]
	ps = [ multiprocessing.Process(target = RealTimeEvents(t,"v5device",fota5DataBase,v5DeviceNeedFields,fota5Schema).loopRun) for t in allFota5DeviceTable]
########################################## region ###############################################################################
	ps += [ multiprocessing.Process(target = RealTimeEvents(t,"v5region",fota5DataBase).loopRun) for t in allFota5RegionTable]
	for p in ps:p.start()
	for p in ps:
		p.join()
	print "Game Over"
