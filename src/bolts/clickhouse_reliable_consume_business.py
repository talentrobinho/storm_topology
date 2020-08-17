#coding=utf-8
import os
import sys
import time
import MySQLdb
import redis
from collections import Counter

from streamparse import Bolt
from streamparse import BatchingBolt
from conndb.conndatabase import ConnDB
from JudgmentBase import JudgmentBusiness, JudgmentFilterCode, JudgmentPidLevel
from SpecialChannelTag import JudgmentChannel


class ConsumeClickHouseBolt(Bolt):
    #config = {'topology.tick.tuple.freq.secs': 1}
    #ticks_between_batches = 10

    def initialize(self, conf, ctx):
        self.unit                   = 100
        self.redis                  = None
        self.tupid                  = None
        self.timestamp              = None
        self.consume                = None
        self.sec                    = None
        self.pid                    = None
        self.business               = None
        #self.key                = 'info_flow_pid'
        self.key                = 'account_type'
        self.account_type_set       = None

        self.msg_count          = 50000
        self.msg_count_ceiling  = 50000
        self.more_values_count      = 0
        self.more_values            = []
        self.write_msg_to_db_line   = 1000

        self.conndb     = ConnDB()
        self.redis      = self.conndb.conn_redis()
        #self.redis      = self.conndb.conn_bak_redis()
        self.mysql      = self.conndb.conn_mysql()
        self.clickhouse = self.conndb.conn_clickhouse()

    def process(self, tup):
        self.loglist        = tup.values
        self.tupid          = tup.id
        self.sec            = str(int(time.time()))

        self.pid                = self.loglist[2]
        self.business           = self.loglist[22]
        self.upos_asid          = self.loglist[33]
        
        self.class_info     = JudgmentBusiness.judgment_business(self.business)
        self.class_level1   = 'UNKNOW' if self.class_info[0] is None else self.class_info[0]
        self.class_level2   = 'UNKNOW' if self.class_info[0] is None else self.class_info[1]
        self.class_level3   = 'UNKNOW' if self.class_info[0] is None else self.class_info[2]

        '''判断PC搜索渠道   Begin'''
        self.pc_business        = 'UNKNOW'

        if self.pc_business == 'UNKNOW':
            self.pc_business = JudgmentChannel.BD(self.pid, self.business)
        if self.pc_business == 'UNKNOW':
            self.pc_business = JudgmentChannel.BRWS(self.pid, self.business)
        if self.pc_business == 'UNKNOW':
            self.pc_business = JudgmentChannel.IME(self.pid, self.business)
        if self.pc_business == 'UNKNOW':
            self.pc_business = JudgmentChannel.SOGOU(self.pid, self.business)
        if self.pc_business == 'UNKNOW':
            self.pc_business = JudgmentChannel.SOSO(self.business)
        if self.pc_business == 'UNKNOW':
            self.pc_business = JudgmentChannel.OneTwoThree(self.pid, self.business)
        if self.pc_business == 'UNKNOW':
            self.pc_business = JudgmentChannel.BC(self.pid, self.upos_asid)
        if self.pc_business == 'UNKNOW':
            self.pc_business = JudgmentChannel.SOHU(self.pid, self.business)
        '''判断PC搜索渠道   End'''


        '''判断是否为本地账号   Begin'''
        self.msg_count += 1
        if self.msg_count > self.msg_count_ceiling:
            #self.info_flow_pid_set = self.redis.smembers(self.key)
            self.account_type_set = self.redis.smembers(self.key)
            self.msg_count = 0

        if self.loglist[6] in self.account_type_set:
            self.account_type = "LOCAL"
        else:
            self.account_type = "CHANNEL"
        '''判断是否为本地账号   END'''

        if self.redis.get(self.tupid) is None:
            self.redis.set(self.tupid, 1, ex=600, nx=True)
            if self.more_values_count >= self.write_msg_to_db_line:
                self.clickhouse.execute("insert into cd_ie_log (server_time, \
                                                              usr_ip, \
                                                              pid, \
                                                              suid_yyid, \
                                                              groupid, \
                                                              adid, \
                                                              accountid, \
                                                              flag, \
                                                              reserved, \
                                                              keyword, \
                                                              deal_time, \
                                                              n, \
                                                              ret, \
                                                              search_keyword, \
                                                              price, \
                                                              pv_refer, \
                                                              clickid, \
                                                              rule_error_msg, \
                                                              pass_error_msg, \
                                                              ip, \
                                                              type, \
                                                              time_consume, \
                                                              service_type, \
                                                              pactype, \
                                                              rollbackret, \
                                                              rollbackopret, \
                                                              extendreserved, \
                                                              creativeid, \
                                                              planid, \
                                                              ma, \
                                                              cx_type, \
                                                              cx_indus, \
                                                              lu, \
                                                              upos_asid, \
                                                              query_reserved, \
                                                              max_price, \
                                                              cost_control_2_pass_type, \
                                                              domain_id, \
                                                              up_id, \
                                                              acid_index, \
                                                              kid_indus, \
                                                              region_public, \
                                                              union_ak, \
                                                              star_first_domain_id, \
                                                              star_second_domain_id, \
                                                              cookie_str, \
                                                              cxid, \
                                                              click_server_res, \
                                                              flow_flag, \
                                                              ssi0, \
                                                              tseq, \
                                                              click_refer, \
                                                              block, \
                                                              style_type, \
                                                              style_id1, \
                                                              style_id2, \
                                                              city_code, \
                                                              search_word_flag, \
                                                              group_cateid, \
                                                              cateid_second, \
                                                              multi_creative_id, \
                                                              location_id, \
                                                              real_price, \
                                                              price_ratio, \
                                                              arr, \
                                                              eesf, \
                                                              pv_time, \
                                                              yhext, \
                                                              imei, \
                                                              query_classfy, \
                                                              show_ip, \
                                                              show_region, \
                                                              domain, \
                                                              apackage, \
                                                              wifimac, \
                                                              androidid, \
                                                              user_agent, \
                                                              rpid, \
                                                              erfs, \
                                                              ipv6, \
                                                              is_ipv6, \
                                                              ie_log_birth, \
                                                              plan_type, \
                                                              gen_log_ip, \
                                                              is_consume_account, \
                                                              optime, \
                                                              business_level1, \
                                                              business_level2, \
                                                              business_level3, \
                                                              pc_business, \
                                                              account_type) values", self.more_values)
                self.more_values_count  = 0
                self.more_values        = []
            else:
                
                self.other_values = (self.sec, 
                                     self.class_level1, 
                                     self.class_level2, 
                                     self.class_level3,
                                     self.pc_business,
                                     self.account_type)

                self.line_values = self.loglist + self.other_values
                self.more_values.append(self.line_values)
                self.more_values_count += 1


