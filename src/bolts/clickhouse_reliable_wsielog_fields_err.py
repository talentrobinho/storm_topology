#coding=utf-8
import os
import sys
import time
import redis

from streamparse import Bolt
from conndb.conndatabase import ConnDB


class WsIeLogFieldErroClickHouseBolt(Bolt):

    def initialize(self, conf, ctx):
        self.tupid                  = None
        self.sec                    = None
        self.redis                  = None


        self.more_values_count      = 0
        self.more_values            = []
        self.write_msg_to_db_line   = 1

        self.conndb     = ConnDB()
        self.clickhouse = self.conndb.conn_clickhouse_cluster()
        self.redis      = self.conndb.conn_redis()

    def process(self, tup):
        self.loglist        = tup.values
        self.tupid          = tup.id
        self.sec            = str(int(time.time()))
    
        if self.redis.get(self.tupid) is None:
            self.redis.set(self.tupid, 1, ex=600, nx=True)
            if self.more_values_count >= self.write_msg_to_db_line:
                self.clickhouse.execute("insert into ws_ie_log_fields_err_all (time, \
                                                                               log_timestamp, \
                                                                               fields_len, \
                                                                               gen_log_ip \
                                                                              ) values", self.more_values)
                self.more_values_count  = 0
                self.more_values        = []
            else:

                self.more_values.append(self.loglist)
                self.more_values_count += 1


