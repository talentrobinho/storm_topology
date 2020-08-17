import os
import linecache
import redis
import time
from collections import Counter

from config.parseconf import ParseConf
from streamparse import Bolt
from streamparse import Stream
from streamparse import BatchingBolt
from conndb.conndatabase import ConnDB


class FilterClickBolt(Bolt):
    '''
    outputs  = [$1, $3, $7, $13, $15, $17, $23, $35, $66]
    outputs = [timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickid', 'srcip', 'bussines', 'queryflag', 'server_ip']
    '''
    outputs = [Stream(fields=['timestamp', 'pid', 'flag', 'reserved', 'filter', 'clickid', 'bussines', 'real_ip', 'server_ip'], name='filter_click_stream')]
    #outputs = [Stream(fields=['timestamp', 'pid', 'flag', 'reserved', 'filter', 'clickid', 'bussines', 'real_ip', 'server_ip'], name='filter_click_stream'),
    #           Stream(fields=['timestamp', 'pid', 'flag', 'reserved', 'filter', 'clickid', 'bussines', 'real_ip', 'server_ip'], name='filter_pass_stream')]



    def initialize(self, conf, ctx):
        self.loglist = []
        self.timestamp = None
        self.sec = None
        self.server_ip = None
        self.log_length = 75
        self.rkey = None

        #self.conndb = ConnDB()
        #self.redis = self.conndb.conn_redis()
        #self.mysql = self.conndb.conn_mysql()

    def process(self, tup):
        self.loglist = tup.values[0].split('\t')
        self.tupid = tup.id
        self.server_ip = tup.values[1]

        if len(self.loglist) < self.log_length:
            pass
        else:
            self.msg = [self.loglist[0], self.loglist[2], self.loglist[7], self.loglist[8], self.loglist[12], self.loglist[16], self.loglist[22], self.loglist[19], self.server_ip]
            #self.logger.info("{} {} {} {} {} {} {} {} {}".format(self.loglist[0], self.loglist[2], self.loglist[7], self.loglist[8], self.loglist[12], self.loglist[16], self.loglist[22], self.loglist[19], self.server_ip))
            #self.logger.info("%s"%">".join(self.msg))
            self.emit(self.msg, stream='filter_click_stream')
            #if self.loglist[12] == "0":
            #    self.emit(self.msg, stream='filter_pass_stream')
            #else:
            #    self.emit(self.msg, stream='filter_click_stream')
            #self.sec = int(time.time())
            #self.rkey = "{}_{}".format('backtype', self.tupid)
            #if self.redis.get(self.rkey) is None:
            #    self.redis.set(self.rkey, 1, ex=1800, nx=True)
            #    self.timestamp = self.loglist[0]

            #self.timestamp = self.loglist[0]
            #self.backtype = self.loglist[72]
            #self.realserver_ip = self.loglist[19]
            #self.bussiness = self.loglist[22]
            #self.logger.info(">>> %s"%(" ".join(self.msg),))
                
