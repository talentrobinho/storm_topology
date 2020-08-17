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
    '''
    out_fields = ['timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickid', 'srcip', 'bussines', 'queryflag', 'server_ip']
    outputs = [Stream(fields=out_fields, name='ws_bidding_qq_stream'),
               Stream(fields=out_fields, name='ws_bidding_total_stream')]

    def initialize(self, conf, ctx):
        self.loglist = None
        self.total_stream = 'ws_bidding_total_stream'
        self.unit = 100
        self.redis = None
        self.tupid = None
        self.timestamp = None
        self.consume = None
        self.sec = None
        
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

            if self.redis.get(self.tupid) is None:
                self.redis.set(self.tupid, 1, ex=600, nx=True)
                try:
                    self.mysql.execute("insert into consume (time, item, itemgroup, value, serverip, sourceip, optime) values (%s,%s,%s,%s,%s,%s,%s)" ,
                                    (self.timestamp, self.item, self.itemgroup, self.consume, self.server_ip, self.realserver_ip ,self.sec))
                except Exception, err:
                    self.logger.error("[lzs] Database connection idle timeout[%s]."%(self.item,))
                    self.conndb.conn.ping(True)
                    self.mysql.execute("insert into consume (time, item, itemgroup, value, serverip, sourceip, optime) values (%s,%s,%s,%s,%s,%s,%s)" ,
                                    (self.timestamp, self.item, self.itemgroup, self.consume, self.server_ip, self.realserver_ip ,self.sec))

class PUContentFilterLogBolt(BaseFilterLogBolt):

    def process(self, tup):
        super(PUContentFilterLogBolt, self).process(tup, '20100', 'pu_content_consume', 'pu_total_consume')

class PUSearchKeyFilterLogBolt(BaseFilterLogBolt):

    def process(self, tup):
        super(PUSearchKeyFilterLogBolt, self).process(tup, '20800', 'pu_searchkey_consume', 'pu_total_consume')

class WUContentFilterLogBolt(BaseFilterLogBolt):

    def process(self, tup):
        super(WUContentFilterLogBolt, self).process(tup, '50101', 'wu_content_consume', 'wu_total_consume')

class WUSearchKeyFilterLogBolt(BaseFilterLogBolt):

    def process(self, tup):
        super(WUSearchKeyFilterLogBolt, self).process(tup, '50300', 'wu_searchkey_consume', 'wu_total_consume')

class AppContentFilterLogBolt(BaseFilterLogBolt):

    def process(self, tup):
        super(AppContentFilterLogBolt, self).process(tup, '51000', 'app_content_consume', 'app_total_consume')

class AppSearchKeyFilterLogBolt(BaseFilterLogBolt):

    def process(self, tup):
        super(AppSearchKeyFilterLogBolt, self).process(tup, '51100', 'app_searchkey_consume', 'app_total_consume')

class DirectFilterLogBolt(BaseFilterLogBolt):

    def process(self, tup):
        for business in ['51070', '50170', '51050', '55150']:
            super(DirectFilterLogBolt, self).process(tup, business, 'union_direct_consume', 'union_total_consume')
