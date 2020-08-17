import os
import linecache
import redis
import time
from collections import Counter

from streamparse import Bolt
from streamparse import Stream
from streamparse import BatchingBolt
from conndb.conndatabase import ConnDB



class BaseFilterLogBolt(Bolt):
    '''
    out_fields  = [$1, $3, $7, $13, $15, $20, $23, $35]
    
    out_fields = ['timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickid', 'srcip', 'bussines', 'queryflag', 'server_ip']
    outputs = [Stream(fields=out_fields, name='ws_bidding_qq_stream'),
               Stream(fields=out_fields, name='ws_bidding_total_stream')]
    '''

    def initialize(self, conf, ctx):
        self.loglist = None
        self.total_stream = 'ws_bidding_total_stream'
        self.unit = 100
        self.redis = None
        self.tupid = None
        self.timestamp = None
        self.consume = None
        self.sec = None
        self.more_values_count = 0
        self.write_msg_to_db_line = 100
        self.more_values = []
        
        self.conndb = ConnDB()
        self.redis = self.conndb.conn_redis()
        self.mysql = self.conndb.conn_mysql()
        
    def process(self, tup, startstr, item, itemgroup):
        self.loglist = tup.values
        if str(self.loglist[7]).startswith(startstr):
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

            #if self.redis.get(self.tupid) is None:
            if self.redis.set(self.tupid, 1, ex=600, nx=True):
                if self.more_values_count >= self.write_msg_to_db_line:
                    try:
                        self.mysql.executemany("insert into consume(time, \
                                                                    item, \
                                                                    itemgroup, \
                                                                    value, \
                                                                    serverip, \
                                                                    sourceip, \
                                                                    optime) \
                                                            values(%s,%s,%s,%s,%s,%s,%s)", self.more_values)
                    except Exception, err:
                        self.logger.error("[lzs] Database connection idle timeout[%s]."%(self.item,))
                        self.conndb.conn.ping(True)
                        self.mysql.executemany("insert into consume(time, \
                                                                    item, \
                                                                    itemgroup, \
                                                                    value, \
                                                                    serverip, \
                                                                    sourceip, \
                                                                    optime) \
                                                            values(%s,%s,%s,%s,%s,%s,%s)", self.more_values)
                    self.more_values_count = 0
                    self.more_values = []
                else:  
                    self.line_values = (self.timestamp, 
                                        self.item, 
                                        self.itemgroup, 
                                        self.consume,
                                        self.server_ip, 
                                        self.realserver_ip, 
                                        self.sec)
                    self.more_values.append(self.line_values)
                    self.more_values_count += 1

class WSQQFilterLogBolt(BaseFilterLogBolt):

    #out_fields = ['timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickid', 'srcip', 'bussines', 'queryflag', 'server_ip']
    ###outputs = [Stream(fields=out_fields, name='ws_bidding_qq_stream'),
    ###           Stream(fields=out_fields, name='ws_bidding_total_stream')]
    #outputs = [Stream(fields=out_fields, name='ws_bidding_qq_stream')]
    def process(self, tup):
        super(WSQQFilterLogBolt, self).process(tup, '302', 'ws_qq_consume', 'ws_total_consume')


class WSSogouFilterLogBolt(BaseFilterLogBolt):

    #out_fields = ['timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickid', 'srcip', 'bussines', 'queryflag', 'server_ip']
    ###outputs = [Stream(fields=out_fields, name='ws_bidding_sogou_stream'),
    ###           Stream(fields=out_fields, name='ws_bidding_total_stream')]
    #outputs = [Stream(fields=out_fields, name='ws_bidding_sogou_stream')]
    def process(self, tup):
        super(WSSogouFilterLogBolt, self).process(tup, '303', 'ws_sogou_consume', 'ws_total_consume')



class WSWaigouFilterLogBolt(BaseFilterLogBolt):

    #out_fields = ['timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickid', 'srcip', 'bussines', 'queryflag', 'server_ip']
    ###outputs = [Stream(fields=out_fields, name='ws_bidding_waigou_stream'),
    ###           Stream(fields=out_fields, name='ws_bidding_total_stream')]
    def process(self, tup):
        super(WSWaigouFilterLogBolt, self).process(tup, '304', 'ws_waigou_consume', 'ws_total_consume')




#class WSQQFilterLogBolt(Bolt):
#    outputs = [Stream(fields=out_fields, name='ws_bidding_qq_stream'),
#               Stream(fields=out_fields, name='ws_bidding_total_stream')]
#
#    def initialize(self, conf, ctx):
#        self.log_list = []
#        self.timestamp = None
#        self.loglist = None
#        
#    def process(self, tup):
#        self.loglist = tup.values
#        if str(self.loglist[6]).startswith('302'):
#            self.emit(self.loglist, stream='ws_bidding_qq_stream')
#            self.emit(self.loglist, stream='ws_bidding_total_stream')
#
