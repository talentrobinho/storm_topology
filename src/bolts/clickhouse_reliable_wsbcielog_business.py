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


class WsBCIeLogClickHouseBolt(Bolt):
    #config = {'topology.tick.tuple.freq.secs': 1}
    #ticks_between_batches = 10

    def initialize(self, conf, ctx):
        self.tupid                  = None
        #self.timestamp              = None
        #self.consume                = None
        self.sec                    = None
        self.pid                    = None
        #self.business               = None
        self.ad_number              = None
        self.is_ad_display          = None
        self.redis                  = None


        self.more_values_count      = 0
        self.more_values            = []
        self.write_msg_to_db_line   = 50

        self.conndb     = ConnDB()
        self.clickhouse = self.conndb.conn_clickhouse_cluster()
        self.redis      = self.conndb.conn_redis()

    def process(self, tup):
        self.loglist        = tup.values
        self.tupid          = tup.id
        self.sec            = str(int(time.time()))

        self.pid            = self.loglist[2]
        self.ad_number      = 0 if self.loglist[5] == '' else len(self.loglist[5].split(','))
        self.is_ad_display  = 0 if self.loglist[5] == '' else 1
    
        if self.redis.get(self.tupid) is None:
            self.redis.set(self.tupid, 1, ex=600, nx=True)
            if self.more_values_count >= self.write_msg_to_db_line:
                self.clickhouse.execute("insert into ws_bc_ie_log_all (time, \
                                                                       request_ip, \
                                                                       pid, \
                                                                       suid_yyid, \
                                                                       groupid, \
                                                                       adid, \
                                                                       accountid, \
                                                                       flag, \
                                                                       reserved, \
                                                                       keyword, \
                                                                       cost, \
                                                                       refer, \
                                                                       search_keyword, \
                                                                       trans_ip, \
                                                                       uuid, \
                                                                       is_rerank_price, \
                                                                       query_reserved, \
                                                                       pvid, \
                                                                       rank, \
                                                                       n, \
                                                                       qc_search_keyword, \
                                                                       front_style, \
                                                                       plan_type, \
                                                                       match_type, \
                                                                       original_ctr, \
                                                                       gg_cost, \
                                                                       p, \
                                                                       w, \
                                                                       business, \
                                                                       final_ctr, \
                                                                       ext_reserve, \
                                                                       creativeid, \
                                                                       billing_price, \
                                                                       quality, \
                                                                       offer_price, \
                                                                       planid, \
                                                                       get_region_way, \
                                                                       pass_ua, \
                                                                       traffic_sign, \
                                                                       retrieve_flag, \
                                                                       starting_price, \
                                                                       keyword_industry, \
                                                                       query_class, \
                                                                       front_channel, \
                                                                       front_Domain_type, \
                                                                       dynamic_subchain, \
                                                                       city_region, \
                                                                       pno, \
                                                                       ad_adjust_rank, \
                                                                       pv1_pvid, \
                                                                       mfp_base64decode, \
                                                                       msesuuid, \
                                                                       style_reserve, \
                                                                       cookie_info, \
                                                                       chn, \
                                                                       front_query_type, \
                                                                       is_https, \
                                                                       sogouhao_pass_siteid, \
                                                                       sogouhao_pass_adposition, \
                                                                       sogouhao_pass_uuid, \
                                                                       sogouhao_pass_token_flag, \
                                                                       rpid, \
                                                                       sogouhao_pass_medical_exp, \
                                                                       hz_param, \
                                                                       flow_test, \
                                                                       sogouhao_searchkey, \
                                                                       sgh_tag, \
                                                                       gen_log_ip, \
                                                                       optime, \
                                                                       ad_number, \
                                                                       is_ad_display) values", self.more_values)
                self.more_values_count  = 0
                self.more_values        = []
            else:
                
                self.other_values = (self.sec, 
                                     self.ad_number, 
                                     self.is_ad_display
                                    )

                self.line_values = self.loglist + self.other_values
                self.more_values.append(self.line_values)
                self.more_values_count += 1


