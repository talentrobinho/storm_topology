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
from judgmentbase import JudgmentBusiness, JudgmentFilterCode, JudgmentPidLevel


class TagClickBolt(Bolt):
    '''
    outputs  = [$1, $3, $7, $13, $15, $17, $23, $35, $66]
    outputs = [timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickid', 'srcip', 'bussines', 'queryflag', 'server_ip']
    '''
    #outputs = [Stream(fields=['timestamp', 'pid', 'flag', 'reserved', 'clickid', 'filter', 'real_ip', 'server_ip'], name='filter_click_stream'),
    #           Stream(fields=['timestamp', 'pid', 'flag', 'reserved', 'clickid', 'filter', 'real_ip', 'server_ip'], name='filter_pass_stream')]
    outputs = [Stream(fields=['timestamp', 'pid', 'flag', 'reserved', 'filter', 'clickid', 'bussines', 'real_ip', 'server_ip'], name='filter_click_stream')]



    def initialize(self, conf, ctx):
        self.loglist = []
        self.tupid = None
        self.sec = None
        self.rkey = None

        self.timestamp = None
        self.pid = None
        self.flag = None
        self.reserved = None
        self.clickid = None
        self.filter_code = None
        self.real_ip = None
        self.server_ip = None
        self.total = None
        self.level = None
        self.levelgroup = None
        self.pid_level_info = None
        self.class_level1 = None
        self.class_level2 = None
        self.class_level3 = None
        self.filter_case = None
        self.class_info = None

        self.conndb = ConnDB()
        self.redis = self.conndb.conn_redis()
        self.mysql = self.conndb.conn_mysql()

    def process(self, tup):
        self.loglist = tup.values
        self.tupid = tup.id

        if self.loglist: 
            self.sec = int(time.time())
            self.rkey = "{}_{}".format('click', self.tupid)
            if self.redis.get(self.rkey) is None:
                self.redis.set(self.rkey, 1, ex=1800, nx=True)

                self.timestamp = self.loglist[0]
                self.pid = self.loglist[1]
                self.flag = int(self.loglist[2])
                self.reserved = int(self.loglist[3])
                self.filter_code = self.loglist[4]
                self.clickid = self.loglist[5]
                self.business = self.loglist[6].strip(' ')
                self.real_ip = self.loglist[7]
                self.server_ip = self.loglist[8]

                #if not (self.flag%2 and self.flag < 16):
                #    if self.flag%2 == 1:
                #        if self.pid in ['sogou', 'sohu'] or self.pid.startswith('sogou-'):
                #            self.level = 'level_1'
                #            self.levelgroup = 'total_search'
                #        else:
                #            self.level = 'level_xml'
                #            self.levelgroup = 'total_xml'
                #    else:
                #        self.level = 'leve_{}'.formate(str((self.reserved & 0x78000)/0x8000))
                #        self.levelgroup = 'total_{}'.format(self.level)

                #    #self.msg = [self.timestamp, self.sec, self,pid, self.flag, self.reserved, self.filter, self.level, self.levelgroup, self.real_ip, self.server_ip]
                #    self.msg = [self.timestamel1, self.level2, self.level3, self.real_ip, self.server_ip]

                #print "ddddddddddd: %s"%self.pid_level_info
                #self.logger.info("{}={} {} {} {} {} {} {} {} {}".format(self.sec, self.timestamp, self.pid, self.flag, self.reserved, self.filter_code, self.clickid, self.business, self.real_ip, self.server_ip))
                self.pid_level_info = JudgmentPidLevel.judgment_pid_level(self.pid, self.flag, self.reserved)
                self.level = self.pid_level_info[0] if self.pid_level_info else None
                #print "xxxxxxxx: %s"%self.pid_level_info
                self.levelgroup = self.pid_level_info[1] if self.pid_level_info else None
                #self.level = self.pid_level_info.pidlevel
                #self.levelgroup = self.pid_level_info.levelgroup

                #self.logger.info("{}={} {} {} {} {} {} {} {} {}".format(self.sec, self.timestamp, self.pid, self.flag, self.reserved, self.filter_code, self.business))
                self.filter_case = JudgmentFilterCode.judgment_filter_code(self.filter_code)
                self.class_info = JudgmentBusiness.judgment_business(self.business)
                #self.class_level1 = self.class_info[0] if not self.class_info else None
                self.class_level1 = 'UNKNOW' if self.class_info[0] is None else self.class_info[0]
                self.class_level2 = 'UNKNOW' if self.class_info[0] is None else self.class_info[1]
                self.class_level3 = 'UNKNOW' if self.class_info[0] is None else self.class_info[2]


                #self.pid_level_info = JudgmentPidLevel.judgment_pid_level(self.pid, self.flag, self.reserved)
                #try:
                #    self.level = self.pid_level_info[0]
                #    self.levelgroup = self.pid_level_info[1]
                #except TypeError:
                #    self.level = None
                #    self.levelgroup = None

                #self.filter_case = JudgmentFilterCode.judgment_filter_code(self.filter_code)

                #try:
                #    self.class_info = JudgmentBusiness.judgment_business(self.business)
                #    self.class_level1 = self.class_info[0]
                #    self.class_level2 = self.class_info[1]
                #    self.class_level3 = self.class_info[2]
                #except TypeError:
                #    self.class_level1 = None
                #    self.class_level2 = None
                #    self.class_level3 = None

                #self.logger.info("{}-{} {} {} {} {} {} {} {} {} {} {}".format(self.sec, self.timestamp, self.filter_code, self.filter_case, self.level, self.levelgroup, self.business, self.class_level1, self.class_level2, self.class_level3, self.real_ip, self.server_ip))
                self.mysql.execute("insert into bill_click (optime, time, filtercode, filtercase, pid, pidlevel, pidlevelgroup, business, businesslevel1, businesslevel2, businesslevel3, clickid, serverip, sourceip) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (self.sec, self.timestamp, self.filter_code, self.filter_case, self.pid, self.level, self.levelgroup, self.business, self.class_level1, self.class_level2, self.class_level3, self.clickid, self.real_ip, self.server_ip))

                

                ##if self.loglist[12] == "0":
                ##    self.emit(self.msg, stream='filter_pass_stream')
                ##else:
                ##    self.emit(self.msg, stream='filter_click_stream')

                #self.timestamp = self.loglist[0]
                #self.backtype = self.loglist[72]
                #self.realserver_ip = self.loglist[19]
                #self.business = self.loglist[22]
                #self.logger.info(">>> %s"%(" ".join(self.msg),))
                
