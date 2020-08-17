import os
import linecache
import redis
import time
import requests
from collections import Counter

from config.parseconf import ParseConf
from streamparse import Bolt
from streamparse import Stream
from streamparse import BatchingBolt
from conndb.conndatabase import ConnDB
from judgmentbase import JudgmentBusiness, JudgmentFilterCode, JudgmentPidLevel


TAOBAO_CHANNEL_KEY = ['ttc_ws_waigou',
                      'ttc_ws_ziyou',
                      'ttc_ps_waigou',
                      'ttc_ps_ziyou']

TAOBAO_CHANNEL = {TAOBAO_CHANNEL_KEY[0]: 'ws_waigou',
                  TAOBAO_CHANNEL_KEY[1]: 'ws_ziyou',
                  TAOBAO_CHANNEL_KEY[2]: 'ps_waigou',
                  TAOBAO_CHANNEL_KEY[3]: 'ps_ziyou'}

class TagWSLogBolt(Bolt):
    '''
    outputs  = [$1, $3, $5, $6, $7, $11, $30, $14]
    outputs = ['timestamp', 'pid', 'adgroupid', 'adid', 'accountid', 'cost', 'bussines', 'real_ip', 'server_ip']
    '''
    outputs = [Stream(fields=['sec','timestamp', 'pid', 'adid_count', 'cost', 'taobao_accountid', 'taobao_channel', 'bussines', 'class1', 'class2', 'class3', 'real_ip', 'server_ip'], name='xxxtest')]


    def initialize(self, conf, ctx):
        self.loglist = []
        self.tupid = None
        self.sec = None
        self.key = 'ws_log'
        self.rkey = None
        self.taobao_account = "taobao_total"
        self.msg_count = 1
        self.msg_count_ceiling = 5000
        self.write_msg_to_db_line = 50

        self.timestamp = None
        self.pid = None
        self.adgroupid = None
        self.adid = None
        self.accountid = None
        self.cost = None
        self.business = None
        self.real_ip = None
        self.server_ip = None
        self.datalist = []

        self.class_level1 = None
        self.class_level2 = None
        self.class_level3 = None
        self.taobao_account_list = set()

        self.conndb = ConnDB()
        self.redis = self.conndb.conn_redis()
        self.mysql = self.conndb.conn_mysql()
        self.influxdb = self.conndb.conn_influxdb()

        self.ws_waigou = self.redis.smembers(TAOBAO_CHANNEL_KEY[0])
        self.ws_ziyou = self.redis.smembers(TAOBAO_CHANNEL_KEY[1])
        self.ps_waigou = self.redis.smembers(TAOBAO_CHANNEL_KEY[2])
        self.ps_ziyou = self.redis.smembers(TAOBAO_CHANNEL_KEY[3])

        self.more_values = []
        self.more_values_count = 1
        self.line_values = None

    def statistical_pv(self, adid_list):
        a = adid_list.strip()
        if a == '':
            return 0
        return len(a.split(','))
        
    def statistical_pv_taobao(self, acid_list, msg_count):
        acid_count = 0
        a = acid_list.strip()
        if a == '':
            return 0
        if msg_count == 0:
            self.taobao_account_list = self.redis.smembers(self.taobao_account)
        for acid in a.split(','):
            if acid.strip() in self.taobao_account_list:
                acid_count+=1
        return acid_count

    def judgment_taobao_channel(self, pid, msg_count):
        if msg_count == 0:
            self.ws_waigou = self.redis.smembers(TAOBAO_CHANNEL_KEY[0])
            self.ws_ziyou = self.redis.smembers(TAOBAO_CHANNEL_KEY[1])
            self.ps_waigou = self.redis.smembers(TAOBAO_CHANNEL_KEY[2])
            self.ps_ziyou = self.redis.smembers(TAOBAO_CHANNEL_KEY[3])
        if pid in self.ws_waigou:
           return TAOBAO_CHANNEL[TAOBAO_CHANNEL_KEY[0]]
        elif pid in self.ws_ziyou:
            return TAOBAO_CHANNEL[TAOBAO_CHANNEL_KEY[1]]
        elif pid in self.ps_waigou:
            return TAOBAO_CHANNEL[TAOBAO_CHANNEL_KEY[2]]
        elif pid in self.ps_ziyou:
            return TAOBAO_CHANNEL[TAOBAO_CHANNEL_KEY[3]]

    def process(self, tup):
        self.loglist = tup.values
        self.tupid = tup.id

        if self.loglist: 
            self.sec = int(time.time())
            self.rkey = "%s_%s"%(self.key, self.tupid)
            if self.redis.set(self.rkey, 1, ex=600, nx=True):

                if self.msg_count > self.msg_count_ceiling:
                    self.msg_count = 0
                 
                self.timestamp = self.loglist[0]
                self.pid = self.loglist[1].strip().strip('\n')
                self.adgroupid = self.loglist[2]
                self.adid = self.loglist[3]
                self.accountid = self.loglist[4]
                self.cost = self.loglist[5]
                self.business = self.loglist[6].strip().strip('\n')
                self.real_ip = self.loglist[7].strip().strip('\n')
                self.server_ip = self.loglist[8]

                self.class_info = JudgmentBusiness.judgment_business(self.business)
                self.class_level1 = 'UNKNOW' if self.class_info[0] is None else self.class_info[0]
                self.class_level2 = 'UNKNOW' if self.class_info[0] is None else self.class_info[1]
                self.class_level3 = 'UNKNOW' if self.class_info[0] is None else self.class_info[2]

                self.adid_count = self.statistical_pv(self.adid)                
                self.taobao_accountid_count = self.statistical_pv_taobao(self.accountid, self.msg_count)                
                self.tmp = self.judgment_taobao_channel(self.pid, self.msg_count)                
                self.taobao_channel = 'UNKNOW' if self.tmp is None else self.tmp

                if self.more_values_count >= self.write_msg_to_db_line:
                    self.mysql.executemany("insert into ws_ie_log(optime, \
                                                                  time, \
                                                                  pid, \
                                                                  adid_count, \
                                                                  cost, \
                                                                  taobao_accountid_count, \
                                                                  taobao_channel, \
                                                                  business, \
                                                                  businesslevel1, \
                                                                  businesslevel2, \
                                                                  businesslevel3, \
                                                                  serverip, \
                                                                  sourceip) \
                                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", self.more_values)
                    self.more_values_count = 0
                    self.more_values = []
                else:
                    self.line_values = (self.sec, 
                                        self.timestamp, 
                                        self.pid, 
                                        self.adid_count,
                                        self.cost, 
                                        self.taobao_accountid_count, 
                                        self.taobao_channel, 
                                        self.business, 
                                        self.class_level1, 
                                        self.class_level2, 
                                        self.class_level3, 
                                        self.real_ip, 
                                        self.server_ip)
                    self.more_values.append(self.line_values)
                    self.more_values_count += 1
                #self.mysql.execute("insert into ws_ie_log (optime, time, pid, adid_count, cost, taobao_accountid_count, taobao_channel, business, businesslevel1, businesslevel2, businesslevel3, serverip, sourceip) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (self.sec, self.timestamp, self.pid, self.adid_count, self.cost, self.taobao_accountid_count, self.taobao_channel, self.business, self.class_level1, self.class_level2, self.class_level3, self.real_ip, self.server_ip))
                #self.logger.info('%s,%s,%s,%d,%s,%d,%s,%s,%s,%s,%s,%s,%s'%(self.sec, self.timestamp, self.pid, self.adid_count, self.cost, self.taobao_accountid_count, self.taobao_channel, self.business, self.class_level1, self.class_level2, self.class_level3, self.real_ip, self.server_ip))
                
#self.conndb.conn.commit()
                #try:
                #    self.mysql.execute("insert into ws_ie_log (optime, time, pid, adid_count, cost, taobao_accountid_count, taobao_channel, business, businesslevel1, businesslevel2, businesslevel3, serverip, sourceip) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (self.sec, self.timestamp, self.pid, self.adid_count, self.cost, self.taobao_accountid_count, self.taobao_channel, self.business, self.class_level1, self.class_level2, self.class_level3, self.real_ip, self.server_ip))
                #    'wsielog,taobao_channel=waigou,businesslevel1=WSSearch,businesslevel2=WaiGou,businesslevel3=QQ,serverip=4.4.4.4,sourceip=3.3.3.3,pid=sogou-wsse-8f2425cd74580acf optime=1532433958,adid_count=54,cost=9876543,taobao_accountid_count=33'
                #     json_body = [
                #{
                #    'measurement': 'wsielog',
                #    'tags': {
                #        'tabao_channel': self.taobao_channel,
                #        'businesslevel1': self.class_level1,
                #        'businesslevel2': self.class_level2,
                #        'businesslevel3': self.class_level3,
                #        'serverip': self.real_ip,
                #        'sourceip': self.server_ip,
                #        'pid': self.pid,
                #    },
                #    'fields': {
                #        'logtime': self.sec,
                #        'optime': self.timestamp,
                #        'adid_count': self.adid_count,
                #        'cost': self.cost,
                #        'taobao_accountid_count': self.taobao_accountid_count,
                #        'business': self.business
                #    }
                #}
                #]
                #self.influxdb.write_points(json_body)
                #data = "wsielog,tabao_channel=%s,businesslevel1=%s,businesslevel2=%s,businesslevel3=%s,serverip=%s,sourceip=%s,pid=%s optime=%d,logtime=%s,adid_count=%s,cost=%s,taobao_accountid_count=%s,business=%s"%(self.taobao_channel, self.class_level1, self.class_level2, self.class_level3, self.real_ip, self.server_ip, self.pid, self.sec, self.timestamp, self.adid_count, self.cost, self.taobao_accountid_count, self.business)
                #    #self.datalist.append(data)
                #    #if len(self.datalist) == 50:
                #    #    self.conndb.write_influxdb('\n'.join(self.datalist))
                #    #    self.datalist = []

                #self.conndb.write_influxdb(data)
                #req = requests.post("http://mysql.storm.adt.sogou:8086/write?db=realtime", data=data)
                #self.logger.info("%s %s"%("http://mysql.storm.adt.sogou:8086/write?db=realtime", data))
                #self.logger.info(req.status_code)
                #self.logger.info(req.headers)
                #    #data = "wsielog,tabao_channel={},businesslevel1={},businesslevel2={},businesslevel3={},serverip={},sourceip={},pid={},optime={},logtime={},adid_count={},cost={},taobao_accountid_count={},business={}".format(self.taobao_channel, self.class_level1, self.class_level2, self.class_level3, self.real_ip, self.server_ip, self.pid, self.sec, self.timestamp, self.adid_count, self.cost, self.taobao_accountid_count, self.business)
                #    #data_list = data.split(',')
                #    #self.emit(data_list, stream='xxxtest')
                #except Exception, error:
                #    self.logger.error("Failed to write data to the database[%s]"%(error,))
                    #self.mysql.execute("create table ws_ie_log(id int NOT NULL AUTO_INCREMENT, time varchar(15) NOT NULL, optime varchar(15) NOT NULL, pid varchar(64) NOT NULL, adid_count int(2) NOT NULL, cost varchar(12) NOT NULL, taobao_accountid_count int(2) NOT NULL,taobao_channel varchar(30) NOT NULL,business varchar(10) NOT NULL, businesslevel1 varchar(30) NOT NULL, businesslevel2 varchar(30) NOT NULL, businesslevel3 varchar(30) NOT NULL, serverip varchar(20) NOT NULL, sourceip varchar(20),PRIMARY KEY (`id`)) ENGINE=innodb default charset=utf8;")
                    #self.conndb.conn.commit()
