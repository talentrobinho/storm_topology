import os
import linecache
import redis
import time
from collections import Counter

from streamparse import Bolt
from streamparse import Stream
from streamparse import BatchingBolt
from conndb.conndatabase import ConnDB



class BaseCommonBolt(Bolt):
    '''
    out_fields  = [$1, $3, $7, $13, $15, $20, $23, $35]
    
    out_fields = ['timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickid', 'srcip', 'bussines', 'queryflag', 'server_ip']
    outputs = [Stream(fields=out_fields, name='ws_bidding_qq_stream'),
               Stream(fields=out_fields, name='ws_bidding_total_stream')]
    '''

    def initialize(self, conf, ctx):
        self.loglist = None
        self.unit = 100
        self.redis = None
        self.tupid = None
        self.timestamp = None
        self.consume = None
        self.sec = None
        self.more_values_count = 0
        self.write_msg_to_db_line = 20
        self.more_values = []
        
        self.conndb = ConnDB()
        self.redis = self.conndb.conn_redis()
        self.mysql = self.conndb.conn_mysql()

        
    def process(self, tup, bstr, mpid, item, itemgroup):
        self.loglist = tup.values
        if mpid.find(','):
            pidlist = mpid.split(',')
        else:
            pidlist = mpid
        for pid in pidlist:
            if str(self.loglist[1]) == pid and str(self.loglist[7]).startswith(bstr):
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
                            self.mysql.executemany("insert into galaxy_consume(time, \
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
                            self.mysql.executemany("insert into galaxy_consume(time, \
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




class GalaxyECBolt(BaseCommonBolt):

    def process(self, tup):
        super(GalaxyECBolt, self).process(tup, \
                                         '30304', \
                                         'sogou-apps-3501672ebc68a552', \
                                         'galaxy_ec_consume', \
                                         'galaxy_total_consume')

class WGFinanceBolt(BaseCommonBolt):

    def process(self, tup):
        super(WGFinanceBolt, self).process(tup, \
                                          '30304', \
                                          'sogou-apps-a35f4223bb8f6c86', \
                                          'wg_finance_consume', \
                                          'wg_total_consume')


class WGOtherBolt(BaseCommonBolt):

    def process(self, tup):
        super(WGOtherBolt, self).process(tup, \
                                        '30304', \
                                        'sogou-waps-c0cccc24dd23ded6,\
                                         sogou-apps-9eb53b5052d534ea,\
                                         sogou-waps-3000311ca56a1cb9,\
                                         sogou-apps-3261769be720b0fe,\
                                         sogou-apps-47e51e9d11cf800f', \
                                        'wg_other_consume', \
                                        'wg_total_consume')

class MoonECBolt(BaseCommonBolt):

    #out_fields = ['timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickid', 'srcip', 'bussines', 'queryflag', 'server_ip']
    ###outputs = [Stream(fields=out_fields, name='ws_bidding_qq_stream'),
    ###           Stream(fields=out_fields, name='ws_bidding_total_stream')]
    #outputs = [Stream(fields=out_fields, name='ws_bidding_qq_stream')]
    def process(self, tup):
        self.loglist = tup.values
        if str(self.loglist[7]).startswith('50760') or str(self.loglist[7]).startswith('20760'):
            self.loglist = tup.values
            self.tupid = tup.id
            self.timestamp = self.loglist[0]
            self.item = 'moonec_consume'
            self.itemgroup = 'moon_consume_total'
            self.consume = round(float(self.loglist[4])/100,2)
            self.server_ip = self.loglist[9]
            self.realserver_ip = self.loglist[6]
            self.sec = int(time.time())
            self.timestamp_src = self.loglist[0]

            #if self.redis.get(self.tupid) is None:
            if self.redis.set(self.tupid, 1, ex=600, nx=True):
                if self.more_values_count >= self.write_msg_to_db_line:
                    try:
                        self.mysql.executemany("insert into galaxy_consume(time, \
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
                        self.mysql.executemany("insert into galaxy_consume(time, \
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

