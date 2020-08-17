import os
import linecache
import redis
import time
import sys
import MySQLdb
import logging
from collections import Counter
from rediscluster import StrictRedisCluster

from streamparse import Bolt
from streamparse import Stream
from streamparse import BatchingBolt
from conndb.conndatabase import ConnDB

class BaseFilterLogBolt(Bolt):

    def initialize(self, conf, ctx):
        self.loglist = None
        self.msg_count = 0
        self.msg_count_ceiling = 10000
        self.taobao_account_set = None
        self.taobao_account = "taobao_total"
        self.more_values_count = 0
        self.write_msg_to_db_line = 100
        self.more_values = []
        self.low_quality_channel_list = ('30304', '30408', '30409')     ### add low quality

        #try:
        #    self.redis = redis.Redis(host='10.134.15.104', db=1)
        #except Exception:
        #    self.logger.error("Filter Error connecting to database.")
        #    sys.exit(1)

        #try:
        #    #self.conn = MySQLdb.connect(host="10.134.115.196",user="monitor",passwd="123456",db="zabbix",port=3306,charset="utf8")
        #    self.conn = MySQLdb.connect(host="mysql.storm.adt.sogou",user="monitor",passwd="123456",db="realtime",port=3306,charset="utf8")
        #    self.cur = self.conn.cursor()
        #except Exception:
        #    self.logger.error("Error connecting to MySQL database.")
        #    sys.exit(1)

        self.conndb = ConnDB()
        self.redis = self.conndb.conn_redis()
        self.mysql = self.conndb.conn_mysql()

        self.taobao_account_set = self.redis.smembers(self.taobao_account)
        
    def process(self, tup, startstr, item, itemgroup):

        '''
            consume table struct
            +-----------+--------------+------+-----+---------+----------------+
            | Field     | Type         | Null | Key | Default | Extra          |
            +-----------+--------------+------+-----+---------+----------------+
            | id        | int(11)      | NO   | PRI | NULL    | auto_increment |
            | time      | varchar(15)  | NO   |     | NULL    |                |
            | item      | varchar(128) | NO   |     | NULL    |                |
            | itemgroup | varchar(128) | NO   |     | NULL    |                |
            | value     | double(15,2) | NO   |     | NULL    |                |
            | serverip  | varchar(20)  | NO   |     | NULL    |                |
            | sourceip  | varchar(20)  | YES  |     | NULL    |                |
            | optime    | varchar(15)  | NO   |     | NULL    |                |
            +-----------+--------------+------+-----+---------+----------------+
        '''
        self.loglist = tup.values
        self.tupid = tup.id

        self.timestamp = self.loglist[0]
        self.item = item
        self.itemgroup = itemgroup
        self.consume = round(float(self.loglist[4])/100,2)
        self.bussines = self.loglist[7]     ### add low quality
        self.server_ip = self.loglist[9]
        self.realserver_ip = self.loglist[6]
        self.sec = int(time.time())
        self.timestamp_src = self.loglist[0]
        self.rkey = None


        #if self.redis.sismember(item, self.loglist[2].strip('\n')):
        if self.msg_count > self.msg_count_ceiling:
            self.taobao_account_set = self.redis.smembers(self.taobao_account)
            self.msg_count == 0
        #if self.redis.sismember(self.taobao_account, self.loglist[2].strip('\n')):
        if self.loglist[2].strip('\n') in self.taobao_account_set:
            self.msg_count += 1
            self.rkey = "{0}_{1}".format("tt", self.tupid)
            if self.redis.set(self.rkey, 1, ex=600, nx=True):
                #self.logger.info("##### %s,%s,%s,%s,%s,%s,%s"%(self.timestamp, self.item, self.itemgroup, self.consume, self.server_ip, self.realserver_ip ,self.sec))
                #self.mysql.execute("insert into consume (time, item, itemgroup, value, serverip, sourceip, optime) values (%s,%s,%s,%s,%s,%s,%s)" , \
                #                  (self.timestamp, self.item, self.itemgroup, self.consume, self.server_ip, self.realserver_ip ,self.sec))
                #try:
                #    self.cur.execute("insert into consume (time, name, consume, op_time) values (%s,%s,%s,%s)" , (self.timestamp, item, self.sum_consume,self.sec))
                #except Exception:
                #    self.logger.error("------------- Insert data to database error.---------------")

                ### add low quality
                ###if self.bussines in self.low_quality_channel_list:
                ###    self.item = 'taobao_low_quality'
                self.item = 'taobao_low_quality' if self.bussines in self.low_quality_channel_list else item
                if self.more_values_count >= self.write_msg_to_db_line:
                    self.mysql.executemany("insert into big_customers_consume(time, \
                                                                item, \
                                                                itemgroup, \
                                                                value, \
                                                                serverip, \
                                                                sourceip, \
                                                                optime) \
                                                        values(%s,%s,%s,%s,%s,%s,%s)", self.more_values)
                    self.more_values_count = 0
                    self.more_values = []
                
                self.line_values = (self.timestamp, 
                                    self.item, 
                                    self.itemgroup, 
                                    self.consume,
                                    self.server_ip, 
                                    self.realserver_ip, 
                                    self.sec)
                self.more_values.append(self.line_values)
                self.more_values_count += 1


class TAOBAOTotalBolt(BaseFilterLogBolt):
    def process(self, tup):
        super(TAOBAOTotalBolt, self).process(tup, '302', 'taobao', 'taobao_total_consume')




###############################################################################################
################### no use
###############################################################################################
class TAOBAOHOTBolt(BaseFilterLogBolt):
    def process(self, tup):
        super(TAOBAOHOTBolt, self).process(tup, '302', 'taobao_taobaohot')

class NETSALEBolt(BaseFilterLogBolt):
    def process(self, tup):
        super(NETSALEBolt, self).process(tup, '302', 'taobao_netsale')


class TMALLBRANDBolt(BaseFilterLogBolt):
    def process(self, tup):
        super(TMALLBolt, self).process(tup, '302', 'taobao_tmallbrand')

class TMALLELECBolt(BaseFilterLogBolt):
    def process(self, tup):
        super(TMALLELECBolt, self).process(tup, '302', 'taobao_tmallelec')


class TMALLPHONEBolt(BaseFilterLogBolt):
    def process(self, tup):
        super(TMALLPHONEBolt, self).process(tup, '302', 'taobao_tmallphone')


class TMALLSUPERBolt(BaseFilterLogBolt):
    def process(self, tup):
        super(TMALLSUPERBolt, self).process(tup, '302', 'taobao_tmallsuper')


class ALIBolt(BaseFilterLogBolt):
    def process(self, tup):
        super(ALIBolt, self).process(tup, '302', 'taobao_ali')

