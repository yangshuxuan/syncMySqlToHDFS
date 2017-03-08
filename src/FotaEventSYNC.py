#!/usr/anaconda2/bin/python
# -*- coding: utf-8 -*-
import random
import sys
import os 
import signal  
import time
import pytz
import datetime
import re
import time

from daemonize import Daemonize
from datetime import date,timedelta
import predictionio
import MySQLdb as mdb

import json
import logging
from hdfs import Config
from hdfs import InsecureClient
from hdfs.ext.avro import AvroReader, AvroWriter
readyQuit=False
def onsignal_usr1(a,b):
    global readyQuit
    readyQuit=True
    print '收到SIGUSR1信号'
signal.signal(signal.SIGUSR1,onsignal_usr1) 

class MIDException(Exception):
  pass
class RealTimeEvents(object):
	def __init__(self,deviceInfoTableName,kind,dataBaseInfo,needFields="*",schema=None):
		self.dataBaseInfo = dataBaseInfo
		self.prefix = deviceInfoTableName
		self.kind = kind
		self.initDir = "/user/ct_fota/YangShuxuanNotDelete"
		self.iniFileName=self.prefix + ".ini"
		self.needFields = needFields
		#self.initLog()
		self.connectDB()
		self.clientHDFS = Config().get_client()
		self.changtimes = 0
		self.schema = schema
	def buildFileName(self):
		pt=self.curID_PT["PT"].strftime('%Y-%m-%d')
		urls = ["http://adups-m01:50070","http://adups-m02:50070"]
		clients = [InsecureClient(url, user='ct_fota') for url in urls] 
		while True:
			destFileName = self.initDir + "/" + pt + "/" + self.kind + "/" + self.prefix + "_" + str(self.curID_PT["FILEID"]) + ".avro"
			statuses = [(client.status(destFileName,strict=False),client) for client in clients]
			if all(status is None or status[u"length"] == 0 or self.testAvroLength(status,destFileName,client) for status,client in statuses):
				return destFileName
			else:
				self.curID_PT["FILEID"] += 1
	def testAvroLength(self,status,destFileName,client):
		if status[u"length"] > 5000 : return False
		with AvroReader(client, destFileName) as reader:
			return len(list(reader)) == 0
	def deleteZeroFile(self):
		curAvroFileName = self.buildFileName()
		urls = ["http://adups-m01:50070","http://adups-m02:50070"]
		clients = [InsecureClient(url, user='ct_fota') for url in urls]
		for client in clients:
			try:
				client.delete(curAvroFileName)
			except:
				pass
	def saveWhere(self):
		ptStr = self.curID_PT["PT"].strftime('%Y-%m-%d')
		idStr = str(self.curID_PT["ID"])
		fileidStr = str(self.curID_PT["FILEID"])
		with open(self.iniFileName,"w") as fp:
			fp.write(idStr + "\n" + ptStr + "\n" + fileidStr)
	def readWhere(self):
		"""
		为何没有在异常里面重置，这有可能导致重复将安装和点击事件导入到hbase中去
		当程序异常退出时，有必要根据当前运行到的位置继续运行
		"""
		self.curID_PT = {}
		try:
			with open(self.iniFileName,"r") as fp:
				self.lastID = self.curID_PT["ID"] = int(fp.readline().strip())
				self.curID_PT["PT"] = datetime.datetime.strptime(fp.readline().strip(),'%Y-%m-%d').date()
				self.curID_PT["FILEID"] = int(fp.readline().strip())
		except IOError:
			self.lastID = self.curID_PT["ID"]  = 0
			self.curID_PT["PT"] = datetime.datetime.strptime("2017-02-18",'%Y-%m-%d').date()
			self.curID_PT["FILEID"] = 0

	def initLog(self):
		fileName = self.prefix + ".log"
		logger=logging.getLogger()
		hdlr=logging.FileHandler(fileName)
		formatter=logging.Formatter('%(asctime)s %(levelname)s %(message)s')
		hdlr.setFormatter(formatter)
		logger.addHandler(hdlr)
		logger.setLevel(logging.NOTSET)
		self.slogger=logger
	def connectDB(self):
		db_ip = self.dataBaseInfo.db_ip
		db_user = self.dataBaseInfo.db_user
		db_passwd = self.dataBaseInfo.db_passwd
		db_base = self.dataBaseInfo.db_base
		db_port = self.dataBaseInfo.db_port
		self.con=mdb.connect(db_ip,db_user,db_passwd,db_base,db_port,charset="utf8")

	def saveToHDFS(self):
		self.readWhere()
		qs="select %s from %s where push_time >= '%s 00:00:00' and push_time < '%s 00:00:00' and id>%d order by id asc;"\
		%(self.needFields,self.prefix,self.curID_PT["PT"].strftime('%Y-%m-%d'),(self.curID_PT["PT"] + timedelta(1)).strftime('%Y-%m-%d'),self.curID_PT["ID"])
		curAvroFileName = self.buildFileName()
		try:
			with self.con,AvroWriter(self.clientHDFS, curAvroFileName, schema=self.schema, overwrite=True) as writer:
				db = self.con.cursor(mdb.cursors.DictCursor)
				db.execute(qs)
				rs=db.fetchall()
				for r in rs:
					#if r['last_checktime']:
					#	r['last_checktime'] = time.mktime(r['last_checktime'].timetuple())
					for k,v in r.items():
						if type(v) == datetime.datetime:
							r[k] = time.mktime(v.timetuple())
					writer.write(r)
					self.curID_PT["ID"]=r["id"]
		except (mdb.Error) as e:
			self.slogger.info(e)
		finally:
			self.slogger.info("Now here:%s\t%d"%(self.curID_PT["PT"].isoformat(),self.curID_PT["ID"]))
			self.saveWhere()
	def changeDate(self):
		if self.lastID==self.curID_PT["ID"] and self.curID_PT["PT"]<date.today():
			self.changtimes += 1
			if self.changtimes == 10:
				self.deleteZeroFile()
				self.curID_PT["PT"]= self.curID_PT["PT"] + timedelta(1)
				self.curID_PT["FILEID"] = 0
				self.changtimes = 0
				self.slogger.info("go on to %s\t%d"%(self.curID_PT["PT"],self.curID_PT["ID"]))
				self.saveWhere()
				if self.curID_PT["PT"] == datetime.datetime.strptime("2017-03-08",'%Y-%m-%d').date():
					return 0.
			return 1.
		else:
			self.lastID = self.curID_PT["ID"]
			self.changtimes = 0
			return 3.
	def loopRun(self):
		self.initLog()
		self.slogger.info('You can gracefully kill -10 %d'%(os.getpid(),))

		while True:
			if readyQuit:
				break		
			self.saveToHDFS()
			t = self.changeDate()
			if t == 0. : break
			time.sleep(t)
			self.checkMysqlState()

	def checkMysqlState(self):
		try:
			self.con.ping(True)
		except mdb.Error,e:
			self.slogger.info(u"数据库失去连接")
			self.connectDB()
	def start(self):
		self.initLog()
		pidfile = self.prefix + ".pid"
		daemon = Daemonize(app = self.prefix, pid=pidfile, action=self.loopRun,auto_close_fds=False,chdir=".",foreground=False,logger=self.slogger)
		daemon.start()