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
    outputs = [Stream(fields=["time",
                              "request_ip",
                              "pid",
                              "sohuid_inputid",
                              "groupid",
                              "adid",
                              "accountid",
                              "flag",
                              "reserved",
                              "keyword",
                              "cost",
                              "click_sohu_url",
                              "ret",
                              "search_keyword",
                              "class_type",
                              "pv_refer",
                              "clickid",
                              "customer_url",
                              "time_diff",
                              "ip",
                              "ac_type",
                              "mouse_data",
                              "service_type",
                              "pid_pass_rule",
                              "jump_type",
                              "extend_reserved",
                              "creativeid",
                              "planid",
                              "req_ip_type",
                              "hostname",
                              "ma",
                              "cxtype",
                              "cxids",
                              "lu",
                              "upos",
                              "origin_adid",
                              "ad_price",
                              "query_reserved",
                              "history_price",
                              "max_price",
                              "price",
                              "domain_id",
                              "is_redirect",
                              "upid",
                              "ca_id",
                              "ck_id",
                              "region_public",
                              "ak",
                              "show_ip",
                              "fd",
                              "sd",
                              "cxid",
                              "sohu_url",
                              "bill_url",
                              "traffic_sign",
                              "union_refer_site",
                              "crown_style",
                              "crown_creativeid",
                              "crown_imagid",
                              "city_code",
                              "newid",
                              "group_cateid",
                              "cateid_second",
                              "multi_creative_id",
                              "location_id",
                              "real_price",
                              "price_ratio",
                              "style_flag",
                              "yhext",
                              "show_region",
                              "query_classfy",
                              "imei",
                              "taobao_imei_ret_type",
                              "x_forwarded_for",
                              "android_id",
                              "mfp",
                              "erfs",
                              "anticheat_charge_res",
                              "rpid",
                              "user_agent",
                              "ipv6",
                              "plan_type",
                              "cookie",
                              "gen_log_ip"], name='clickhouse_tag_billielog_stream')]
             # Stream(fields=['timestamp', 'log_timestamp', 'fields_len', 'server_ip'], name='field_err_stream')]


    def initialize(self, conf, ctx):
        self.line_log           = []
        self.loglist            = []
        self.timestamp          = None
        self.loginfo            = None
        self.server_ip          = None
        self.log_length         = 83
        #self.msg_count          = 1
        #self.msg_count_ceiling  = 500000

        #self.conndb = ConnDB()
        #self.redis  = self.conndb.conn_redis()

        #self.account_result_set = self.redis.smembers(self.key)

    def _split_log(self,loginfo):
        if loginfo.values[0]:
            self.line_log = loginfo.values[0].split('\n')
        return self.line_log
            

    def process(self, tup):

            self.loglist    = tup.values[0].split('\t')
            self.server_ip  = tup.values[1]
            
            info_len =  len(self.loglist)         
            #self.logger.info(info_len)
            if info_len >= self.log_length:
                
                #self.emit([self.timestamp, self.loglist[0], info_len, self.server_ip], stream='field_err_stream')

                new_loglist = self.loglist[0:self.log_length]
                new_loglist.append(self.server_ip) 
                #self.logger.info(new_loglist)
                #self.logger.info(len(new_loglist))
                #self.logger.info('=============')
                self.emit(new_loglist, stream='clickhouse_tag_billielog_stream')
            #else:
            #    self.loglist.append(self.server_ip) 
                #self.logger.info(self.loglist) 
             #   self.emit(self.loglist, stream='clickhouse_tag_billielog_stream')
                        