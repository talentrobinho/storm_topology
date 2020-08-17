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

class FilterLogClickHouseBolt(Bolt):
    '''
    outputs  = [$1, $3, $7, $13, $15, $17, $23, $35, $66]
    outputs = [timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickid', 'srcip', 'bussines', 'queryflag', 'server_ip']
    '''
    outputs = [Stream(fields=["time",
                              "an",
                              "message",
                              "type",
                              "cluster",
                              "message_status",
                              "gen_log_ip",], name='clickhouse_tag_wsups_updatelog_stream')]
             # Stream(fields=['timestamp', 'log_timestamp', 'fields_len', 'server_ip'], name='field_err_stream')]


    def initialize(self, conf, ctx):
        self.line_log           = []
        self.loglist            = []
        self.timestamp          = None
        self.loginfo            = None
        self.server_ip          = None
        self.log_length         = 5
        self.message_status     = None


            

    def process(self, tup):

            self.loglist    = tup.values[0].replace('\3', '\t').split('\t')
            self.server_ip  = tup.values[1]
            
            info_len =  len(self.loglist) 
            #self.logger.info(self.loglist)        
            if info_len < self.log_length:
                #self.emit([self.timestamp, self.loglist[0], info_len, self.server_ip], stream='field_err_stream')
                pass
            elif info_len > self.log_length and self.loglist[1] == '1':
                if self.loglist[3] == 'CK':
                    try:
                        self.message_status = self.loglist[20]
                    except IndexError as err:
                        self.logger.error("{}".format(err))
                        self.message_status = 'UNKOWN'
                else: 
                    self.message_status = 'UNKOWN'
                new_loglist = [self.loglist[0], 
                               self.loglist[2], 
                               self.loglist[3], 
                               self.loglist[4], 
                               self.loglist[-1],
                               self.message_status,
                               self.server_ip]
                self.emit(new_loglist, stream='clickhouse_tag_wsups_updatelog_stream')

                        