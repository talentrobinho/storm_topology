import os
import linecache
import redis
import time
import re
from collections import Counter

from config.parseconf import ParseConf
from streamparse import Bolt
from streamparse import Stream
from streamparse import BatchingBolt
from conndb.conndatabase import ConnDB


#TAOBAO_HOT_SALE_URL = ['uland.taobao.com/']
TAOBAO_HOT_SALE_URL = re.compile(r'redirect\.simba.*https*%3A%2F%2Fuland\.taobao\.com')
TAOBAO_HOT_SALE_KW = re.compile(r'keyword%3D&|keyword%3D$|keyword%3D%26')
'''
redirect\.simba.*https*%3A%2F%2Fuland\.taobao\.com
              grep -P 'keyword%3D&'
              grep -P 'keyword%3D$'
              grep -P 'keyword%3D%26'
'''
#NET_SALE_URL = ['p4psearch.1688.com/', 'http://view.1688.com']
NET_SALE_URL = re.compile(r'1688\.com')
NET_SALE_KW = re.compile(r'keyword%3D&|keyword%3D$|keyword%3D%26')
'''
grep 1688
              grep -P 'keyword%3D&'
              grep -P 'keyword%3D$'
              grep -P 'keyword%3D%26'
'''
#TMALL_URL = ['tmall.com/']
TMALL_URL = re.compile(r'redirect\.simba.*http%3A%2F%2Fs\.click\.taobao\.com')

'''
grep -P 'redirect\.simba.*http%3A%2F%2Fs\.click\.taobao\.com'
'''

class FilterTTLPBolt(Bolt):

    def initialize(self, conf, ctx):
        self.loglist = []
        self.timestamp = None
        self.sec = None
        self.server_ip = None
        self.log_length = 75
        self.rkey = None
        self.tt = None
        self.landingpage = None

        self.conndb = ConnDB()
        #self.redis = self.conndb.conn_redis()
        self.mysql = self.conndb.conn_mysql()

    def process(self, tup):
        self.sec = int(time.time())
        self.loglist = tup.values[0].split('\t')
        self.tupid = tup.id
        self.server_ip = tup.values[1]

        if len(self.loglist) < self.log_length:
            return
        self.timestamp = self.loglist[0]
        self.url = self.loglist[17]
        self.real_ip = self.loglist[19]

        #if self.url.find(TAOBAO_HOT_SALE_URL[0]) >= 0 and self.url.find('keyword') >= 0:
        if TAOBAO_HOT_SALE_URL.search(self.url) and not TAOBAO_HOT_SALE_KW.search(self.url):
        #if TAOBAO_HOT_SALE_URL.search(self.url):
            #self.tt = self.url.split('keyword=')[1]
            #if self.tt.strip() != "":
            self.landingpage = "TAOBAO_HOT_SALE"
            self.mysql.execute("insert into taobao_landingpage (optime, time, landingpage, serverip, sourceip) values (%s,%s,%s,%s,%s)"
                             ,(self.sec, self.timestamp, self.landingpage, self.real_ip, self.server_ip))
        #elif self.url.find(NET_SALE_URL[1]) >= 0 and self.url.find('1688APP') >= 0:
        #elif NET_SALE_URL.search(self.url):
        #    self.landingpage = "NET_SALE"
        #    self.mysql.execute("insert into taobao_landingpage (optime, time, landingpage, serverip, sourceip) values (%s,%s,%s,%s,%s)"
        #                     ,(self.sec, self.timestamp, self.landingpage, self.real_ip, self.server_ip))
        #elif self.url.find(NET_SALE_URL[0]) >= 0 and self.url.find('keywords') >= 0:
        elif NET_SALE_URL.search(self.url) and not NET_SALE_KW.search(self.url):
            #self.tt = self.url.split('keywords=')[1]
            #if self.tt.strip() != "":
            self.landingpage = "NET_SALE"
            #self.logger.info(self.url)
            self.mysql.execute("insert into taobao_landingpage (optime, time, landingpage, serverip, sourceip) values (%s,%s,%s,%s,%s)"
                             ,(self.sec, self.timestamp, self.landingpage, self.real_ip, self.server_ip))
        #elif self.url.find(TMALL_URL[0]) >= 0:
        elif TMALL_URL.search('self.url'):
            self.landingpage = "TMALL"
            self.mysql.execute("insert into taobao_landingpage (optime, time, landingpage, serverip, sourceip) values (%s,%s,%s,%s,%s)"
                             ,(self.sec, self.timestamp, self.landingpage, self.real_ip, self.server_ip))
