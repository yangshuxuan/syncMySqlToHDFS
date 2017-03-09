#!/usr/anaconda2/bin/python
# -*- coding: utf-8 -*-

import multiprocessing
import sys
import logging

from commands import getstatusoutput
sys.path.append("/home/ct_fota/YangShuxuan/OptFotaSalesCount")
from customPyPackageYSX.FotaEventSYNC import RealTimeEvents	
from customPyPackageYSX.configInfo import *
from customPyPackageYSX.DataBaseConf import *
# def readTables(dataBaseInfo,tableName):
	# db_ip = self.dataBaseInfo.db_ip
	# db_user = self.dataBaseInfo.db_user
	# db_passwd = self.dataBaseInfo.db_passwd
	# db_base = self.dataBaseInfo.db_base
	# db_port = self.dataBaseInfo.db_port
	# self.con=mdb.connect(db_ip,db_user,db_passwd,db_base,db_port,charset="utf8")
	# qs="select %s from %s where push_time >= '%s 00:00:00' and push_time < '%s 00:00:00' and id>%d order by id asc;"\
		# %(self.needFields,self.prefix,self.curID_PT["PT"].strftime('%Y-%m-%d'),(self.curID_PT["PT"] + timedelta(1)).strftime('%Y-%m-%d'),self.curID_PT["ID"])
	# try:
		# with self.con:
		# db = self.con.cursor(mdb.cursors.DictCursor)
			# db.execute(qs)
			# rs=db.fetchall()
			# for r in rs:
				# #if r['last_checktime']:
				# #	r['last_checktime'] = time.mktime(r['last_checktime'].timetuple())
				# for k,v in r.items():
					# if type(v) == datetime.datetime:
						# r[k] = time.mktime(v.timetuple())
				# writer.write(r)
				# self.curID_PT["ID"]=r["id"]
	# except (mdb.Error) as e:
		# self.slogger.info(e)
	# finally:
		# con.close()
def initLog():
	fileName = "schedule.log"
	logger=logging.getLogger()
	hdlr=logging.FileHandler(fileName)
	formatter=logging.Formatter('%(asctime)s %(levelname)s %(message)s')
	hdlr.setFormatter(formatter)
	logger.addHandler(hdlr)
	logger.setLevel(logging.NOTSET)
	return logger
if __name__=="__main__":
	logger = initLog()
	logger.info(u"Begin pull data ------- 开始拉数据")

	ps = []
#########################################fota 3.0###############################################################################
	ps = [ multiprocessing.Process(target = RealTimeEvents(t,"v3device",fota3DataBase,v3DeviceNeedFields,fota3Schema).loopRun) for t in allFota3DeviceTable]
	########################################## fota 3.0 region ###############################################################################
	ps += [ multiprocessing.Process(target = RealTimeEvents(t,"v3region",fota3DataBase).loopRun) for t in allFota3RegionTable]

#################################  fota4 device        ########################################
	ps += [ multiprocessing.Process(target = RealTimeEvents(t,"v4device",fota4DataBase,v4DeviceNeedFields,fota4Schema).loopRun) for t in allFota4TableNames]
	
	#################################  fota4 old device        ########################################
	ps.append(multiprocessing.Process(target = RealTimeEvents("deviceInfo","oldv4device",fota4DataBase,oldv4DeviceNeedFields,oldv4deviceschema).loopRun))
	
	################################   fota4 open device           #################################################
	ps.append(multiprocessing.Process(target = RealTimeEvents("deviceopen","v4deviceopen",fota4DataBase,openDeviceNeedFields,deviceopenschema).loopRun))
	
	################################   fota4 pad              ##########################################################
	ps.append(multiprocessing.Process(target = RealTimeEvents("deviceInfo_pad","v4deviceInfo_pad",fota4DataBase,v4DeviceNeedFields,fota4Schema).loopRun))
	
	################################   fota4 other device     ###################################################
	ps.append(multiprocessing.Process(target = RealTimeEvents("deviceInfoother","v4deviceInfoother",fota4DataBase,v4DeviceNeedFields,fota4Schema).loopRun))	
		
	###################################### fota4 region ###############################################################################
	ps += [ multiprocessing.Process(target = RealTimeEvents(t,"v4region",fota4DataBase).loopRun) for t in allFota4RegionTable]

########################################## fota 5.0 device  ###############################################################################
	ps += [ multiprocessing.Process(target = RealTimeEvents(t,"v5device",fota5DataBase,v5DeviceNeedFields,fota5Schema).loopRun) for t in allFota5DeviceTable]
	###################################### fota 5.0 region   ###############################################################################
	ps += [ multiprocessing.Process(target = RealTimeEvents(t,"v5region",fota5DataBase).loopRun) for t in allFota5RegionTable]	
	
	for p in ps:p.start()
	for p in ps:
		p.join()
	logger.info(u"End pull data ------- 结束拉数据")
	status,output=getstatusoutput(sparkJobCMD)
	logger.info(u"End spark job ------- 结束spark作业")
