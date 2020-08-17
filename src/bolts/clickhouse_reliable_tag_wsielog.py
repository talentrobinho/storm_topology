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
                              "suid_yyid",
                              "groupid",
                              "adid",
                              "accountid",
                              "flag",
                              "reserved",
                              "keyword",
                              "cost",
                              "refer",
                              "search_keyword",
                              "trans_ip",
                              "uuid",
                              "is_rerank_price",
                              "query_reserved",
                              "pvid",
                              "rank",
                              "n",
                              "qc_searchkey",
                              "front_style",
                              "rerank_flag",
                              "zhisou_flag",
                              "time_consuming",
                              "health",
                              "p",
                              "w",
                              "material_triple",
                              "business",
                              "original_ctr",
                              "ext_reserve",
                              "creativeId",
                              "billing_price",
                              "quality",
                              "offer_price",
                              "planid",
                              "get_region_way",
                              "pv_tags",
                              "ad_style",
                              "return_len",
                              "pass_ua",
                              "traffic_sign",
                              "retrieve_flag",
                              "starting_price",
                              "keyword_industry",
                              "query_class",
                              "front_channel",
                              "front_Domain_type",
                              "dynamic_subchain",
                              "city_region",
                              "pno",
                              "request_file",
                              "ad_candidate_style",
                              "match_type",
                              "mfp_base64decode",
                              "msesuuid",
                              "style_reserve",
                              "cookie_info",
                              "chn",
                              "guid",
                              "is_https",
                              "teston",
                              "puuid" ,
                              "convert_sogou_free",
                              "final_ctr",
                              "credibility",
                              "miaf",
                              "city_code",
                              "is_belowad_frontcode",
                              "galaxy_flag",
                              "pid_in_service_type_file",
                              "dp_and_rc",
                              "web_api_search",
                              "sogouhao_pass_uuid",
                              "sogouhao_pass_siteid",
                              "sogouhao_pass_adposition",
                              "is_logo",
                              "artid_gid",
                              "huazhang_ad",
                              "rpid",
                              "idmap",
                              "zhisou_type",
                              "bigsearch_test",
                              "plan_type",
                              "gen_log_ip"], name='clickhouse_tag_wsielog_stream')]


    def initialize(self, conf, ctx):
        self.line_log           = []
        self.loglist            = []
        self.log_info_list      = []
        self.timestamp          = None
        self.key                = 'account_result'
        self.loginfo            = None
        self.server_ip          = None
        self.log_length         = 86
        self.msg_count          = 1
        self.msg_count_ceiling  = 500000

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
                        
            if len(self.loglist) < self.log_length:
                pass
            else:
                self.loglist.append(self.server_ip) 
                #self.logger.info(self.loglist) 
                self.emit(self.loglist, stream='clickhouse_tag_wsielog_stream')
                        