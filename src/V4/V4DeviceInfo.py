#!/usr/anaconda2/bin/python
# -*- coding: utf-8 -*-
from configInfo import *
import multiprocessing
import sys
sys.path.append("/home/ct_fota/YangShuxuan")
from customPyPackageYSX.FotaEventSYNC import RealTimeEvents	
	
if __name__=="__main__":
	ps = []
	#################################  fota4 device        ########################################
	ps = [ multiprocessing.Process(target = RealTimeEvents(t,"v4device",fota4DataBase,v4DeviceNeedFields,fota4Schema).loopRun) for t in allFota4TableNames]
	
	#################################  fota4 old device        ########################################
	ps.append(multiprocessing.Process(target = RealTimeEvents("deviceInfo","oldv4device",fota4DataBase,oldv4DeviceNeedFields,oldv4deviceschema).loopRun))
	
	################################   fota4 open device           #################################################
	ps.append(multiprocessing.Process(target = RealTimeEvents("deviceopen","v4deviceopen",fota4DataBase,openDeviceNeedFields,deviceopenschema).loopRun))
	
	################################   fota4 pad              ##########################################################
	ps.append(multiprocessing.Process(target = RealTimeEvents("deviceInfo_pad","v4deviceInfo_pad",fota4DataBase,v4DeviceNeedFields,fota4Schema).loopRun))
	
	################################   fota4 other device     ###################################################
	ps.append(multiprocessing.Process(target = RealTimeEvents("deviceInfoother","v4deviceInfoother",fota4DataBase,v4DeviceNeedFields,fota4Schema).loopRun))	
	

	
########################################## region ###############################################################################
	ps += [ multiprocessing.Process(target = RealTimeEvents(t,"v4region",fota4DataBase).loopRun) for t in allFota4RegionTable]
	for p in ps:p.start()
	for p in ps:
		p.join()
	print "Game Over"
