#--coding: utf-8 --
import os
import sys
import linecache
import redis
import time
from collections import Counter

from config.parseconf import ParseConf
from streamparse import Bolt
from streamparse import Stream
from streamparse import BatchingBolt
from conndb.conndatabase import ConnDB

reload(sys)
sys.setdefaultencoding( "utf-8" )

class FilterLogClickHouseBolt(Bolt):
    '''
    outputs  = [$1, $3, $7, $13, $15, $17, $23, $35, $66]
    outputs = [timestamp', 'pid', 'accountid', 'effclick', 'price', 'clickid', 'srcip', 'bussines', 'queryflag', 'server_ip']
    '''
    outputs = [Stream(fields=["server_time",
                              "usr_ip",
                              "pid",
                              "suid_yyid",
                              "groupid",
                              "adid",
                              "accountid",
                              "flag",
                              "reserved",
                              "keyword",
                              "deal_time",
                              "n",
                              "ret",
                              "search_keyword",
                              "price",
                              "pv_refer",
                              "clickid",
                              "rule_error_msg",
                              "pass_error_msg",
                              "ip",
                              "type",
                              "time_consume",
                              "service_type",
                              "pactype",
                              "rollbackret",
                              "rollbackopret",
                              "extendreserved",
                              "creativeid",
                              "planid",
                              "ma",
                              "cx_type",
                              "cx_indus",
                              "lu",
                              "upos_asid",
                              "query_reserved",
                              "max_price",
                              "cost_control_2_pass_type",
                              "domain_id",
                              "up_id",
                              "acid_index",
                              "kid_indus",
                              "region_public",
                              "union_ak",
                              "star_first_domain_id",
                              "star_second_domain_id",
                              "cookie_str",
                              "cxid",
                              "click_server_res",
                              "flow_flag",
                              "ssi0",
                              "tseq",
                              "click_refer",
                              "block",
                              "style_type",
                              "style_id1",
                              "style_id2",
                              "city_code",
                              "search_word_flag",
                              "group_cateid",
                              "cateid_second",
                              "multi_creative_id",
                              "location_id",
                              "real_price",
                              "price_ratio",
                              "arr",
                              "eesf",
                              "pv_time",
                              "yhext",
                              "imei",
                              "query_classfy",
                              "show_ip",
                              "show_region",
                              "domain",
                              "apackage",
                              "wifimac",
                              "androidid",
                              "user_agent",
                              "rpid",
                              "erfs",
                              "ipv6",
                              "is_ipv6", 
                              "ie_log_birth", 
                              "plan_type",
                              "gen_log_ip",
                              "is_consume_account"], name='clickhouse_tag_cdielog_stream')]


    def initialize(self, conf, ctx):
        self.line_log           = []
        self.loglist            = []
        self.log_info_list      = []
        self.timestamp          = None
        self.key                = 'account_result'
        self.account_type       = 'account_type'
        self.loginfo            = None
        self.server_ip          = None
        self.log_length         = 83
        self.msg_count          = 1
        self.msg_count_ceiling  = 100000
        self.is_consume_account = 0

        self.conndb = ConnDB()
        self.redis  = self.conndb.conn_redis()

        self.account_result_set = self.redis.smembers(self.key)

    def _split_log(self,loginfo):
        if loginfo.values[0]:
            self.line_log = loginfo.values[0].split('\n')
        return self.line_log
            

    def process(self, tup):

            self.loglist    = tup.values[0].split('\t')
            self.server_ip  = tup.values[1]
            #self.account_result_flag = 0
            self.is_consume_account = 0
                        
            if len(self.loglist) < self.log_length:
                pass
            else:

                if self.msg_count > self.msg_count_ceiling:
                    self.account_result_set = self.redis.smembers(self.key)
                    self.account_type_set = self.redis.smembers(self.account_type)
                    self.msg_count = 0

                
                if self.loglist[6] in self.account_result_set:
                    self.is_consume_account = 1

                    #if int(self.loglist[12]) == 0:
                self.msg_count += 1
                self.loglist = self.loglist + [self.server_ip, self.is_consume_account]
                self.emit(self.loglist, stream='clickhouse_tag_cdielog_stream')
                        