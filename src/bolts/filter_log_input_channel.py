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
    #'''
    #out_fields  = [$1, $3, $7, $13, $15, $20, $23, $35]
    #'''
    #out_fields = ['timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickid', 'srcip', 'bussines', 'queryflag', 'server_ip']
    #outputs = [Stream(fields=out_fields, name='ws_bidding_qq_stream'),
    #           Stream(fields=out_fields, name='ws_bidding_total_stream')]

    def initialize(self, conf, ctx):
        self.loglist = None
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
            self.pid = self.loglist[1]
            self.item = item
            self.itemgroup = itemgroup
            self.consume = round(float(self.loglist[4])/100,2)
            self.server_ip = self.loglist[9]
            self.realserver_ip = self.loglist[6]
            self.business = self.loglist[7]
            self.sec = int(time.time())
            self.timestamp_src = self.loglist[0]

            #if self.redis.get(self.tupid) is None:
            if self.redis.set(self.tupid, 1, ex=600, nx=True):
                if startstr.find("801") == 0 and self.pid == "sogou-apps-c64e7fcb17df2cd4":
                    self.item = 'input_tongtou_consume'
                # self.mysql.execute("insert into consume (time, item, itemgroup, value, serverip, sourceip, optime) values (%s,%s,%s,%s,%s,%s,%s)" ,
                                  # (self.timestamp, self.item, self.itemgroup, self.consume, self.server_ip, self.realserver_ip ,self.sec))


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

class InputChatFilterLogBolt(BaseFilterLogBolt):

    def process(self, tup):
        super(InputChatFilterLogBolt, self).process(tup, '801', 'input_chat_consume', 'input_total_consume')


class InputDirectFilterLogBolt(BaseFilterLogBolt):

    def process(self, tup):
        super(InputDirectFilterLogBolt, self).process(tup, '802', 'input_direct_consume', 'input_total_consume')



class InputRecommFilterLogBolt(BaseFilterLogBolt):

    def process(self, tup):
        super(InputRecommFilterLogBolt, self).process(tup, '803', 'input_recomm_consume', 'input_total_consume')


class InputGuangGuangFilterLogBolt(BaseFilterLogBolt):

    def process(self, tup):
        super(InputGuangGuangFilterLogBolt, self).process(tup, '804', 'input_guangguang_consume_consume', 'input_total_consume')
