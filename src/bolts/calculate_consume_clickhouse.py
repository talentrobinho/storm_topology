import os
import sys
import time
import MySQLdb
import redis
from collections import Counter

from streamparse import Bolt
from streamparse import BatchingBolt
from conndb.conndatabase import ConnDB
from judgmentbase import JudgmentBusiness, JudgmentFilterCode, JudgmentPidLevel


class BaseConsumeBolt(Bolt):
    #config = {'topology.tick.tuple.freq.secs': 1}
    #ticks_between_batches = 10

    def initialize(self, conf, ctx):
        self.unit                   = 100
        self.redis                  = None
        self.tupid                  = None
        self.timestamp              = None
        self.consume                = None
        self.sec                    = None
        self.more_values_count      = 0
        self.write_msg_to_db_line   = 100

        self.conndb     = ConnDB()
        self.redis      = self.conndb.conn_redis()
        self.mysql      = self.conndb.conn_mysql()
        self.clickhouse = self.conndb.conn_clickhouse()

    def process(self, tup, item, itemgroup):
        self.loglist        = tup.values
        self.tupid          = tup.id
        self.sec            = int(time.time())
        self.timestamp      = self.loglist[0]
        self.pid            = self.loglist[1]
        self.groupid        = self.loglist[2]
        self.adid           = self.loglist[3]
        self.accountid      = self.loglist[4]
        self.is_pass        = self.loglist[5]
        self.price          = round(float(self.loglist[6])/100,2)
        self.clickid        = self.loglist[7]
        self.realserver_ip  = self.loglist[8]
        self.business       = self.loglist[9].strip().strip('\n')
        self.queryflag      = self.loglist[10]
        self.server_ip      = self.loglist[11]

        
        self.class_info     = JudgmentBusiness.judgment_business(self.business)
        self.class_level1   = 'UNKNOW' if self.class_info[0] is None else self.class_info[0]
        self.class_level2   = 'UNKNOW' if self.class_info[0] is None else self.class_info[1]
        self.class_level3   = 'UNKNOW' if self.class_info[0] is None else self.class_info[2]

        if self.redis.get(self.tupid) is None:
            self.redis.set(self.tupid, 1, ex=600, nx=True)
            if self.more_values_count >= self.write_msg_to_db_line:
                self.clickhouse.execute("insert into consume (optime, \
                                                              time, \
                                                              pid, \
                                                              groupid, \
                                                              adid, \
                                                              accountid, \
                                                              is_pass, \
                                                              price, \
                                                              clickid, \
                                                              business, \
                                                              queryflag， \
                                                              businesslevel1, \
                                                              businesslevel2, \
                                                              businesslevel3, \
                                                              serverip, \
                                                              sourceip) values", self.more_values)
                self.more_values_count  = 0
                self.more_values        = []
            else:
                
                self.line_values = [self.sec, 
                                    self.timestamp, 
                                    self.pid, 
                                    self.groupid,
                                    self.adid, 
                                    self.accountid, 
                                    self.is_pass, 
                                    self.price,
                                    self.clickid,
                                    self.business, 
                                    self.queryflag,
                                    self.class_level1, 
                                    self.class_level2, 
                                    self.class_level3, 
                                    self.realserver_ip, 
                                    self.server_ip]
                self.more_values.append(self.line_values)
                self.more_values_count += 1


