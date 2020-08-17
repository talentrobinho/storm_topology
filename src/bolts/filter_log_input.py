#--coding: utf-8 --
import os
import sys
import linecache
import redis
import time
from collections import Counter

from config.parseconf import ParseConf
from streamparse import Bolt
from streamparse import Stream
from streamparse import BatchingBolt
from conndb.conndatabase import ConnDB

reload(sys)
sys.setdefaultencoding( "utf-8" )

class FilterLogInputBolt(Bolt):
    '''
    outputs  = [$1, $3, $7, $13, $15, $17, $23, $35, $66]
    outputs = [timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickid', 'srcip', 'bussines', 'queryflag', 'server_ip']
    '''
    #outputs = [Stream(fields=['timestamp', 'pid', 'accountid', 'effclick', 'consume', 'clickid', 'srcip', 'bussines', 'queryflag', 'server_ip'], name='xxtaobao_filter_log_stream')]
    outputs = [Stream(fields=['timestamp', 'pid', 'accountid', 'effclick', 'consume', 'clickid', 'srcip', 'bussines', 'queryflag', 'server_ip'], name='filter_input_log_stream')]
    #outputs = [Stream(fields=['log_info'], name='filter_log_stream')]

    def initialize(self, conf, ctx):
        self.line_log = []
        self.loglist = []
        self.log_info_list = []
        self.timestamp = None
        self.key = 'account_result'
        self.loginfo = None
        self.server_ip = None
        self.log_length = 77
        self.msg_count = 1
        self.msg_count_ceiling = 500000

        self.conndb = ConnDB()
        self.redis = self.conndb.conn_redis()

        self.account_result_set = self.redis.smembers(self.key)

    def _split_log(self,loginfo):
        if loginfo.values[0]:
            self.line_log = loginfo.values[0].split('\n')
        return self.line_log
            

    def process(self, tup):

            self.loglist = tup.values[0].split('\t')
            self.server_ip = tup.values[1]
            
            #self.ack(tup.id)
            #self.logger.info("------------ server ip: %s %s ------------"%(type(self.loglist), len(self.loglist)))
            if len(self.loglist) < self.log_length:
                pass
            else:

                if self.msg_count > self.msg_count_ceiling:
                    self.account_result_set = self.redis.smembers(self.key)
                    self.msg_count = 0

                #if self.redis.sismember(self.key, self.loglist[6].strip().strip('\n')):
                if self.loglist[6] in self.account_result_set:

                    self.msg_count += 1
                    if int(self.loglist[12]) == 0:
                        '''PC search sub-channel consumption'''
                        self.msg = [self.loglist[0], 
                                    self.loglist[2], 
                                    self.loglist[6], 
                                    self.loglist[12], 
                                    self.loglist[14], 
                                    self.loglist[16], 
                                    self.loglist[19], 
                                    self.loglist[22], 
                                    self.loglist[34], 
                                    self.server_ip]

                        self.emit(self.msg, stream='filter_input_log_stream')
                        