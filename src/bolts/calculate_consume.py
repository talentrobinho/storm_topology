import os
import sys
import time
import MySQLdb
import redis
from collections import Counter

from streamparse import Bolt
from streamparse import BatchingBolt
from conndb.conndatabase import ConnDB


class BaseConsumeBolt(Bolt):
    #config = {'topology.tick.tuple.freq.secs': 1}
    #ticks_between_batches = 10

    def initialize(self, conf, ctx):
        self.unit = 100
        self.redis = None
        self.tupid = None
        self.timestamp = None
        self.consume = None
        self.sec = None

        self.conndb = ConnDB()
        self.redis = self.conndb.conn_redis()
        self.mysql = self.conndb.conn_mysql()

    def process(self, tup, item, itemgroup):
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

        if self.redis.get(self.tupid) is None:
            self.redis.set(self.tupid, 1, ex=1800, nx=True)
            #self.logger.info("###%s,%s,%s###" % (self.sec, ",".join(self.info), item))
            ###self.reconn_mysql()
            #self.cur.execute("insert into consume (time, name, consume, op_time) values (%s,%s,%s,%s)" , (self.info[0], item, float(self.info[4])/100, self.sec))
            #self.logger.info(">>> %s,%s,%s,%s,%s,%s,%s"%(self.timestamp, self.item, self.itemgroup, self.consume, self.server_ip, self.realserver_ip ,self.sec))
            try:
                self.mysql.execute("insert into consume (time, item, itemgroup, value, serverip, sourceip, optime) values (%s,%s,%s,%s,%s,%s,%s)" ,
                                  (self.timestamp, self.item, self.itemgroup, self.consume, self.server_ip, self.realserver_ip ,self.sec))
            except Exception, err:
                self.logger.error("[lzs] Database connection idle timeout[%s]."%(self.item,))
                self.conndb.conn.ping(True)
                self.mysql.execute("insert into consume (time, item, itemgroup, value, serverip, sourceip, optime) values (%s,%s,%s,%s,%s,%s,%s)" ,
                                  (self.timestamp, self.item, self.itemgroup, self.consume, self.server_ip, self.realserver_ip ,self.sec))
            #try:
            #    self.reconn_mysql()
            #    self.cur.execute("insert into consume (time, name, consume, op_time, log_time) values (%s,%s,%s,%s,%s)" , (self.info[0], item, float(self.info[4])/100, self.sec, self.info[0]))
            #except Exception, err:
            #    self.conndb = ConnDB()
            #    self.mysql = self.conndb.conn_mysql()
            #    self.logger.error("Insert data to database error.")
            #    self.logger.error(err.message)


class WSConsumeBolt(BaseConsumeBolt):

    def process(self, tup):
        super(WSConsumeBolt, self).process(tup, 'ws_total_consume')

class WSQQConsumeBolt(BaseConsumeBolt):

    def process(self, tup):
        super(WSQQConsumeBolt, self).process(tup, 'ws_qq_consume', 'ws_total_consume')

class WSSOGOUConsumeBolt(BaseConsumeBolt):

    def process(self, tup):
        super(WSSOGOUConsumeBolt, self).process(tup, 'ws_sogou_consume', 'ws_total_consume')

class WSWAIGOUConsumeBolt(BaseConsumeBolt):

    def process(self, tup):
        super(WSWAIGOUConsumeBolt, self).process(tup, 'ws_waigou_consume', 'ws_total_consume')





class PCConsumeBolt(BaseConsumeBolt):

    def process(self, tup):
        super(PCConsumeBolt, self).process(tup, 'pc_total_consume')


class PCSOHUConsumeBolt(BaseConsumeBolt):

    def process(self, tup):
        super(PCSOHUConsumeBolt, self).process(tup, 'pc_sohu_consume', 'pc_total_consume')


class PCSOGOUConsumeBolt(BaseConsumeBolt):

    def process(self, tup):
        super(PCSOGOUConsumeBolt, self).process(tup, 'pc_sogou_consume', 'pc_total_consume')


class PCSOSOConsumeBolt(BaseConsumeBolt):

    def process(self, tup):
        super(PCSOSOConsumeBolt, self).process(tup, 'pc_soso_consume', 'pc_total_consume')


class PCOTTConsumeBolt(BaseConsumeBolt):

    def process(self, tup):
        super(PCOTTConsumeBolt, self).process(tup, 'pc_123_consume', 'pc_total_consume')


class PCBDConsumeBolt(BaseConsumeBolt):

    def process(self, tup):
        super(PCBDConsumeBolt, self).process(tup, 'pc_bd_consume', 'pc_total_consume')


class PCBCConsumeBolt(BaseConsumeBolt):

    def process(self, tup):
        super(PCBCConsumeBolt, self).process(tup, 'pc_bc_consume', 'pc_total_consume')


class PCBRWSConsumeBolt(BaseConsumeBolt):

    def process(self, tup):
        super(PCBRWSConsumeBolt, self).process(tup, 'pc_brws_consume', 'pc_total_consume')


class PCIMEConsumeBolt(BaseConsumeBolt):

    def process(self, tup):
        super(PCIMEConsumeBolt, self).process(tup, 'pc_ime_consume', 'pc_total_consume')

