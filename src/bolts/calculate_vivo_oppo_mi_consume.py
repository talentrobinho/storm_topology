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
    '''
    out_fields  = [$1, $3, $7, $13, $15, $20, $23, $35]
    '''
    #out_fields = ['timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickid', 'srcip', 'bussines', 'queryflag', 'server_ip']
    #outputs = [Stream(fields=out_fields, name='taobao_stream')]

    def initialize(self, conf, ctx):
        self.loglist = None
        self.msg_count = 99999
        self.msg_count_ceiling = 10000
        self.more_values_count = 0
        self.write_msg_to_db_line = 100
        self.more_values = []
        self.pid_set = None

        self.conndb = ConnDB()
        self.redis = self.conndb.conn_redis()
        self.mysql = self.conndb.conn_mysql()
        
    def process(self, tup, topic, item, itemgroup):

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
        self.server_ip = self.loglist[9]
        self.realserver_ip = self.loglist[6]
        self.sec = int(time.time())
        self.timestamp_src = self.loglist[0]
        self.rkey = None
        self.pid_list = topic

        #if self.redis.sismember(self.pid_list, self.loglist[1].strip('\n')):
        if self.msg_count > self.msg_count_ceiling:
            self.pid_set = self.redis.smembers(self.pid_list)
            self.msg_count == 0

        if self.loglist[1].strip('\n') in self.pid_set:
            self.msg_count += 1
            self.rkey = "{0}_{1}".format(topic, self.tupid)
            if self.redis.set(self.rkey, 1, ex=600, nx=True):
                #try:
                #    self.mysql.execute("insert into big_customers_consume (time, item, itemgroup, value, serverip, sourceip, optime) values (%s,%s,%s,%s,%s,%s,%s)" ,
                #                      (self.timestamp, self.item, self.itemgroup, self.consume, self.server_ip, self.realserver_ip ,self.sec))
                #except Exception:
                #    self.logger.error("[lzs] Database connection idle timeout[%s]."%(self.item,))
                #    self.conndb.conn.ping(True)
                #    self.mysql.execute("insert into big_customers_consume (time, item, itemgroup, value, serverip, sourceip, optime) values (%s,%s,%s,%s,%s,%s,%s)" ,
                #                      (self.timestamp, self.item, self.itemgroup, self.consume, self.server_ip, self.realserver_ip ,self.sec))

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

class VIVOTotalBolt(BaseFilterLogBolt):
    def process(self, tup):
        super(VIVOTotalBolt, self).process(tup, 'pid_vivo', 'vivo', 'vivo_total')

class OPPOTotalBolt(BaseFilterLogBolt):
    def process(self, tup):
        super(OPPOTotalBolt, self).process(tup, 'pid_oppo', 'oppo', 'oppo_total')

class MITotalBolt(BaseFilterLogBolt):
    def process(self, tup):
        super(MITotalBolt, self).process(tup, 'pid_mi', 'mi', 'mi_total')
