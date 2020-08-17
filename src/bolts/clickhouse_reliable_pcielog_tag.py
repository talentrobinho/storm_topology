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
                              "ip",
                              "pid",
                              "suid",
                              "groupid",
                              "adid",
                              "accountid",
                              "flag",
                              "reserved",
                              "ad_key",
                              "cost",
                              "refer",
                              "query_words",
                              "front_ip",
                              "uuid",
                              "customer_list",
                              "query_words_status",
                              "pvid",
                              "ad_rank_list",
                              "price",
                              "word",
                              "google_top",
                              "google_below",
                              "google_right",
                              "ota_ec_price_rate_list",
                              "request_google",
                              "p_parameter",
                              "m_parameter",
                              "rele_weight",
                              "business_types",
                              "ad_ctr_list",
                              "extend_reserved",
                              "cid_list",
                              "showprice",
                              "quality_value_list",
                              "maxprice",
                              "planid",
                              "quality_list",
                              "sig",
                              "show_brand",
                              "adr_mid_price",
                              "ext_query_reserver",
                              "retrieve_flag",
                              "requested_ad_space",
                              "qt_sogou",
                              "list_of_lowest_bids",
                              "adr_flag",
                              "keyword_list",
                              "traffic_label",
                              "show_strategy",
                              "nflag",
                              "query_classify_res",
                              "extend_flag",
                              "retry_num",
                              "city_code",
                              "lingxi_channel_info",
                              "style_reserved",
                              "budget_price_list",
                              "shg_price",
                              "price_ratio_list",
                              "new_extend_flag",
                              "game_ret_flag",
                              "game_flag",
                              "remarketing_premium_ratio",
                              "ecad_ret_flag",
                              "otaad_ret_flagota",
                              "ota_style",
                              "dacu_price_rate_list",
                              "tuxiu_info",
                              "quality_through_the_log",
                              "leads_marketing_premium_list",
                              "baichuan_revised_advertisement",
                              "style_optimization",
                              "round_ex_log",
                              "style_ctr_log",
                              "final_style_log",
                              "handler_info_url_ipv6",
                              "loginfo_price_ratio_log",
                              "loginfo_match_type_log",
                              "loginfo_plan_type_log",
                              "handler_info_user_health",
                              "gen_log_ip"], name='clickhouse_tag_pcielog_stream'),
              Stream(fields=['timestamp', 'log_timestamp', 'fields_len', 'server_ip'], name='field_err_stream')]


    def initialize(self, conf, ctx):
        self.line_log           = []
        self.loglist            = []
        #self.log_info_list      = []
        self.timestamp          = None
        #self.key                = 'account_result'
        self.loginfo            = None
        self.server_ip          = None
        self.log_length         = 81
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
            if info_len < self.log_length:
                self.emit([self.timestamp, self.loglist[0], info_len, self.server_ip], stream='field_err_stream')
            elif info_len > self.log_length:
                
                self.emit([self.timestamp, self.loglist[0], info_len, self.server_ip], stream='field_err_stream')

                new_loglist = self.loglist[0:self.log_length]
                new_loglist.append(self.server_ip) 
                self.emit(new_loglist, stream='clickhouse_tag_pcielog_stream')
            else:
                self.loglist.append(self.server_ip) 
                #self.logger.info(self.loglist) 
                self.emit(self.loglist, stream='clickhouse_tag_pcielog_stream')
                        