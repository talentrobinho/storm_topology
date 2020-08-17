import os
import linecache
import redis
import time
from collections import Counter

from streamparse import Bolt
from streamparse import Stream
from streamparse import BatchingBolt


class OneTwoThreeFilterLogBolt(Bolt):
    '''
    outputs  = [$1, $3, $7, $13, $15, $17, $20, $23, $35]
    outputs = ['timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickip', 'srcip', 'bussines', 'queryflag']
    '''
    output_field = ['timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickip', 'srcip', 'bussines', 'queryflag', 'serverip']
    outputs = [Stream(fields=output_field, name='pc_bidding_123_stream'),
               Stream(fields=output_field, name='pc_bidding_total_stream')]

    def initialize(self, conf, ctx):
        self.loglist = None
        self.timestamp = None

    def process(self, tup):
        self.loglist = tup.values
        if str(self.loglist[1]).startswith('sogou-navi-'):
            self.emit(self.loglist, stream='pc_bidding_123_stream')
            self.emit(self.loglist, stream='pc_bidding_total_stream')


class BCFilterLogBolt(Bolt):
    '''
    outputs  = [$1, $3, $7, $13, $15, $17, $20, $23, $35]
    outputs = ['timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickip', 'srcip', 'bussines', 'queryflag']
    '''
    output_field = ['timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickip', 'srcip', 'bussines', 'queryflag', 'serverip']
    outputs = [Stream(fields=output_field, name='pc_bidding_bc_stream'),
               Stream(fields=output_field, name='pc_bidding_total_stream')]

    def initialize(self, conf, ctx):
        self.loglist = None
        self.timestamp = None

    def process(self, tup):
        self.loglist = tup.values
        if str(self.loglist[7]).startswith('1') and (int(self.loglist[8]) >> 30) & 1 == 1:
            self.emit(self.loglist, stream='pc_bidding_bc_stream')
            self.emit(self.loglist, stream='pc_bidding_total_stream')


class BDFilterLogBolt(Bolt):
    '''
    outputs  = [$1, $3, $7, $13, $15, $17, $20, $23, $35]
    outputs = ['timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickip', 'srcip', 'bussines', 'queryflag']
    '''
    output_field = ['timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickip', 'srcip', 'bussines', 'queryflag', 'serverip']
    outputs = [Stream(fields=output_field, name='pc_bidding_bd_stream'),
               Stream(fields=output_field, name='pc_bidding_total_stream')]

    def initialize(self, conf, ctx):
        self.loglist = None
        self.timestamp = None

    def process(self, tup):
        self.loglist = tup.values
        if str(self.loglist[7]) in ['10300', '10301'] and str(self.loglist[1]) not in ['sogou', 'sogou-nopid', 'sogou-wrongpid']:
            self.emit(self.loglist, stream='pc_bidding_bd_stream')
            self.emit(self.loglist, stream='pc_bidding_total_stream')


class BrwsFilterLogBolt(Bolt):
    '''
    outputs  = [$1, $3, $7, $13, $15, $17, $20, $23, $35]
    outputs = ['timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickip', 'srcip', 'bussines', 'queryflag']
    '''
    output_field = ['timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickip', 'srcip', 'bussines', 'queryflag', 'serverip']
    outputs = [Stream(fields=output_field, name='pc_bidding_brws_stream'),
               Stream(fields=output_field, name='pc_bidding_total_stream')]

    def initialize(self, conf, ctx):
        self.loglist = None
        self.timestamp = None

    def process(self, tup):
        self.loglist = tup.values
        if (str(self.loglist[7]) == '10400' and not str(self.loglist[1]).startswith('sogou-misc-85bcf11c65e51c2a')) or \
           (str(self.loglist[7]) == '10401' and str(self.loglist[1]) not in ['sogou', 'sohu']):
            self.emit(self.loglist, stream='pc_bidding_brws_stream')
            self.emit(self.loglist, stream='pc_bidding_total_stream')



class IMEFilterLogBolt(Bolt):
    '''
    outputs  = [$1, $3, $7, $13, $15, $17, $20, $23, $35]
    outputs = ['timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickip', 'srcip', 'bussines', 'queryflag']
    '''
    output_field = ['timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickip', 'srcip', 'bussines', 'queryflag', 'serverip']
    outputs = [Stream(fields=output_field, name='pc_bidding_ime_stream'),
               Stream(fields=output_field, name='pc_bidding_total_stream')]

    def initialize(self, conf, ctx):
        self.loglist = None
        self.timestamp = None

    def process(self, tup):
        self.loglist = tup.values
        if str(self.loglist[7]) == '10500' and \
             str(self.loglist[1]).find('sogou-clse-0dc94ddee6d0b5cf') != 0 and \
             str(self.loglist[1]).find('sogou-clse-eda5b489bc6d0b7d') != 0 and \
             str(self.loglist[1]).find('sogou-clse-f26a1be24a9ff7bb') != 0:
            self.emit(self.loglist, stream='pc_bidding_ime_stream')
            self.emit(self.loglist, stream='pc_bidding_total_stream')


class SOSOFilterLogBolt(Bolt):
    '''
    outputs  = [$1, $3, $7, $13, $15, $17, $20, $23, $35]
    outputs = ['timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickip', 'srcip', 'bussines', 'queryflag']
    '''
    output_field = ['timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickip', 'srcip', 'bussines', 'queryflag', 'serverip']
    outputs = [Stream(fields=output_field, name='pc_bidding_soso_stream'),
               Stream(fields=output_field, name='pc_bidding_total_stream')]

    def initialize(self, conf, ctx):
        self.loglist = None
        self.timestamp = None

    def process(self, tup):
        self.loglist = tup.values
        if str(self.loglist[7]) == '10103':
            self.emit(self.loglist, stream='pc_bidding_soso_stream')
            self.emit(self.loglist, stream='pc_bidding_total_stream')


class SogouFilterLogBolt(Bolt):
    '''
    outputs  = [$1, $3, $7, $13, $15, $17, $20, $23, $35]
    outputs = ['timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickip', 'srcip', 'bussines', 'queryflag']
    '''
    output_field = ['timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickip', 'srcip', 'bussines', 'queryflag', 'serverip']
    outputs = [Stream(fields=output_field, name='pc_bidding_sogou_stream'),
               Stream(fields=output_field, name='pc_bidding_total_stream')]

    def initialize(self, conf, ctx):
        self.loglist = None
        self.timestamp = None
        

    def process(self, tup):
        self.loglist = tup.values
        if str(self.loglist[7]) == '10100' or str(self.loglist[1]) in ['sogou', 'sogou-nopid', 'sogou-wrongpid']:
            self.emit(self.loglist, stream='pc_bidding_sogou_stream')
            self.emit(self.loglist, stream='pc_bidding_total_stream')


class SohuFilterLogBolt(Bolt):
    '''
    outputs  = [$1, $3, $7, $13, $15, $17, $20, $23, $35]
    outputs = ['timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickip', 'srcip', 'bussines', 'queryflag']
    '''
    output_field = ['timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickip', 'srcip', 'bussines', 'queryflag', 'serverip']
    outputs = [Stream(fields=output_field, name='pc_bidding_sohu_stream'),
               Stream(fields=output_field, name='pc_bidding_total_stream')]

    def initialize(self, conf, ctx):
        self.loglist = None
        self.timestamp = None
        

    def process(self, tup):
        self.loglist = tup.values
        if str(self.loglist[7]) == '10200' or str(self.loglist[1]) == 'sohu':
            self.emit(self.loglist, stream='pc_bidding_sohu_stream')
            self.emit(self.loglist, stream='pc_bidding_total_stream')
