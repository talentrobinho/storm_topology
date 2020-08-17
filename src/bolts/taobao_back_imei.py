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


BACK_TYPE = {'0': 'NO_TAOBAO_OR_NO_SGM',
             '1': 'NO_IMEI',
             '2': 'FROM_FRONT_IMEI',
             '3': 'FROM_ANDROIDID_IMEI',
             '4': 'FROM_WUID_IMEI'}

class FilterBackTypeBolt(Bolt):
    '''
    outputs  = [$1, $3, $7, $13, $15, $17, $23, $35, $66]
    outputs = [timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickid', 'srcip', 'bussines', 'queryflag', 'server_ip']
    '''
    #outputs = [Stream(fields=['timestamp', 'pid', 'accountid', 'effclick', 'consume', 'clickid', 'srcip', 'bussines', 'queryflag', 'server_ip'], name='xxtaobao_filter_log_stream')]
    outputs = [Stream(fields=['timestamp', 'pid', 'accountid', 'effclick', 'consume', 'clickid', 'srcip', 'bussines', 'queryflag', 'server_ip'], name='filter_log_stream')]
    #outputs = [Stream(fields=['log_info'], name='filter_log_stream')]



    def initialize(self, conf, ctx):
        self.loglist = []
        self.timestamp = None
        self.sec = None
        self.server_ip = None
        self.log_length = 75
        self.rkey = None

        self.conndb = ConnDB()
        self.redis = self.conndb.conn_redis()
        self.mysql = self.conndb.conn_mysql()

    def process(self, tup):
        self.loglist = tup.values[0].split('\t')
        self.tupid = tup.id
        self.server_ip = tup.values[1]

        if len(self.loglist) < self.log_length:
            pass
        elif self.loglist[12] == "0":
            self.sec = int(time.time())
            self.rkey = "{}_{}".format('backtype', self.tupid)
            if self.redis.get(self.rkey) is None:
                self.redis.set(self.rkey, 1, ex=1800, nx=True)
                self.timestamp = self.loglist[0]

                if self.loglist[72] in BACK_TYPE.keys():
                    '''PC search sub-channel consumption'''
                    #self.msg = [self.loglist[0], self.loglist[2], self.loglist[6], self.loglist[12], self.loglist[19], self.loglist[22], self.loglist[72],self.server_ip]
                    #self.msg = [self.loglist[0], self.sec, self.loglist[19], self.loglist[22], self.loglist[72],self.server_ip]
                    self.timestamp = self.loglist[0]
                    self.backtype = self.loglist[72]
                    self.realserver_ip = self.loglist[19]
                    self.bussiness = self.loglist[22]
                    #self.logger.info(">>> %s"%(" ".join(self.msg),))
                    self.mysql.execute("insert into taobao_backtype (time, backtype, bussiness, serverip, sourceip, optime) values (%s,%s,%s,%s,%s,%s)",
                                       (self.timestamp, self.backtype, self.bussiness, self.server_ip, self.realserver_ip ,self.sec))
