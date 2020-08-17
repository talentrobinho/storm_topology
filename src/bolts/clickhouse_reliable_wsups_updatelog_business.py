#coding=utf-8
import os
import sys
import time
import MySQLdb
import redis
from collections import Counter

from streamparse import Bolt
from streamparse import BatchingBolt
from conndb.conndatabase import ConnDB




class WsUpdateUpdateLogClickHouseBolt(Bolt):
    #config = {'topology.tick.tuple.freq.secs': 1}
    #ticks_between_batches = 10

    def initialize(self, conf, ctx):
        self.tupid                  = None
        self.sec                    = None
        self.redis                  = None


        self.more_values_count      = 0
        self.more_values            = []
        self.write_msg_to_db_line   = 1000

        self.conndb     = ConnDB()
        self.clickhouse = self.conndb.conn_clickhouse_cluster()
        self.redis      = self.conndb.conn_redis()

    def process(self, tup):
        self.loglist        = tup.values
        self.tupid          = tup.id
        self.sec            = str(int(time.time()))
        #self.logger.info("check redis ... {}".format(time.time()))
        #if self.redis.get(self.tupid) is None:
            #self.logger.info("set redis ... {}".format(time.time()))
        #msg_id = "UpdateLog{}".format(str(self.tupid))
        #if self.redis.set(msg_id, 1, ex=300, nx=True):
        if self.more_values_count >= self.write_msg_to_db_line:
            #self.logger.info("start insert ... {}".format(time.time()))
            #self.logger.info("--------- {} -------".format(self.tupid))
            self.clickhouse.execute("insert into ws_update_update_log_all (time, \
                                                                           an, \
                                                                           message, \
                                                                           type, \
                                                                           cluster, \
                                                                           message_status, \
                                                                           gen_log_ip, \
                                                                           optime) values", self.more_values)
            self.more_values_count  = 0
            self.more_values        = []
            #self.logger.info("end insert ... {}".format(time.time()))
        else:
            self.line_values = self.loglist + (self.sec,)
            #self.logger.info(self.line_values)
            self.more_values.append(self.line_values)
            self.more_values_count += 1
        #else:
        #    self.logger.info("--------- repeat： {} -------".format(self.tupid))


