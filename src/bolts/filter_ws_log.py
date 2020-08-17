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


class FilterWSLogBolt(Bolt):
    '''
    outputs  = [$1, $3, $5, $6, $7, $11, $30, $14]
    outputs = ['timestamp', 'pid', 'adgroupid', 'adid', 'accountid', 'cost', 'bussines', 'real_ip', 'server_ip']
    '''
    outputs = [Stream(fields=['optime', \
                              'requestip', \
                              'pid', \
                              'suyy_id', \
                              'groupid', \
                              'adid', \
                              'accountid', \
                              'flag', \
                              'reserved', \
                              'keyword', \
                              'cost', \
                              'refer', \
                              'inquire_word', \
                              'transip', \
                              'uuid', \
                              'rerank_price', \
                              'query_reserved', \
                              'pvid', \
                              'rank', \
                              'price', \
                              'qc_inquire_word', \
                              'front_type', \
                              'strategy_rerank', \
                              'zhisou_flag', \
                              'all_cost', \
                              'health', \
                              'p', \
                              'w', \
                              'none', \
                              'business_code', \
                              'original_ctr', \
                              'area_obtaining_method', \
                              'pv_tags', \
                              'ad_type', \
                              'return_content_length', \
                              'front_ua', \
                              'flow_flag', \
                              'retrieve_flag', \
                              'start_price', \
                              'keyword_business', \
                              'inquire_word_type', \
                              'front_channel_number', \
                              'front_domain_type', \
                              'return_dynamic_subchain_id', \
                              'city_region', \
                              'pno', \
                              'requestfile', \
                              'candidate_ad', \
                              'baichuan_pvtype', \
                              'mfp', \
                              'msesuuid', \
                              'style_reserve', \
                              'cookie_info', \
                              'chn', \
                              'guid', \
                              'is_https', \
                              'teston', \
                              'puuid', \
                              'convertSogoufree', \
                              'finally_ctr', \
                              'credibility', \
                              'miaf', \
                              'lbs_return_city_code', \
                              'below_ad', \
                              'galaxy_return_flag', \
                              'is_in_service_type_file', \
                              'dp_rc', \
                              'web_api_search', \
                              'sogouhao_pass_uuid', \
                              'sogouhao_pass_siteid', \
                              'sogouhao_pass_ad_place', \
                              'is_logo', \
                              'jinshu_articleid', \
                              'public_articleid', \
                              'rpid', \
                              'idmap', \
                              'record_zhisou_flow', \
                              'server_ip'], \
                              ##name='filter_ws_log_stream_influxdb')]
                              name='filter_ws_log_stream')]



    def initialize(self, conf, ctx):
        self.loglist = []
        self.timestamp = None
        self.sec = None
        self.server_ip = None
        self.log_length = 72
        self.rkey = None

        #self.conndb = ConnDB()
        #self.redis = self.conndb.conn_redis()
        #self.mysql = self.conndb.conn_mysql()

    def process(self, tup):
        self.loglist = tup.values[0].split('\t')
        self.tupid = tup.id
        self.server_ip = tup.values[1]

        if len(self.loglist) < self.log_length:
            pass
        else:
            self.loglist.append(self.server_ip)
            self.msg = self.loglist
            ###clickhouse
            self.emit(self.msg, stream='filter_ws_log_stream')
            ###influxdb
            ###self.emit(self.msg, stream='filter_ws_log_stream_influxdb')
                
