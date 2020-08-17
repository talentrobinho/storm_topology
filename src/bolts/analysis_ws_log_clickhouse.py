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


    def initialize(self, conf, ctx):
        self.loglist = []
        self.tupid = None
        self.sec = None
        self.key = 'ws_log'
        self.rkey = None
        self.taobao_account = "taobao_total"
        self.msg_count = 1
        self.msg_count_ceiling = 600000
        self.write_msg_to_db_line = 2000

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
        self.clickhouse = self.conndb.conn_clickhouse()

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
            self.time = str(int(time.time()))
            self.rkey = "%s_%s"%(self.key, self.tupid)
            if self.redis.set(self.rkey, 1, ex=600, nx=True):

                if self.msg_count > self.msg_count_ceiling:
                    self.msg_count = 0

                #self.timestamp = self.loglist[0]
                #self.pid = self.loglist[1].strip().strip('\n')
                #self.adgroupid = self.loglist[2]
                #self.adid = self.loglist[3]
                #self.accountid = self.loglist[4]
                #self.cost = self.loglist[5]
                #self.business = self.loglist[6].strip().strip('\n')
                #self.real_ip = self.loglist[7].strip().strip('\n')
                #self.server_ip = self.loglist[8]

                
                self.optime, \
                self.requestip, \
                self.pid, \
                self.suyy_id, \
                self.groupid, \
                self.adid, \
                self.accountid, \
                self.flag, \
                self.reserved, \
                self.keyword, \
                self.cost, \
                self.refer, \
                self.inquire_word, \
                self.transip, \
                self.uuid, \
                self.rerank_price, \
                self.query_reserved, \
                self.pvid, \
                self.rank, \
                self.price, \
                self.qc_inquire_word, \
                self.front_type, \
                self.strategy_rerank, \
                self.zhisou_flag, \
                self.all_cost, \
                self.health, \
                self.p, \
                self.w, \
                self.none, \
                self.business_code, \
                self.original_ctr, \
                self.area_obtaining_method, \
                self.pv_tags, \
                self.ad_type, \
                self.return_content_length, \
                self.front_ua, \
                self.flow_flag, \
                self.retrieve_flag, \
                self.start_price, \
                self.keyword_business, \
                self.inquire_word_type, \
                self.front_channel_number, \
                self.front_domain_type, \
                self.return_dynamic_subchain_id, \
                self.city_region, \
                self.pno, \
                self.requestfile, \
                self.candidate_ad, \
                self.baichuan_pvtype, \
                self.mfp, \
                self.msesuuid, \
                self.style_reserve, \
                self.cookie_info, \
                self.chn, \
                self.guid, \
                self.is_https, \
                self.teston, \
                self.puuid, \
                self.convertSogoufree, \
                self.finally_ctr, \
                self.credibility, \
                self.miaf, \
                self.lbs_return_city_code, \
                self.below_ad, \
                self.galaxy_return_flag, \
                self.is_in_service_type_file, \
                self.dp_rc, \
                self.web_api_search, \
                self.sogouhao_pass_uuid, \
                self.sogouhao_pass_siteid, \
                self.sogouhao_pass_ad_place, \
                self.is_logo, \
                self.jinshu_articleid, \
                self.public_articleid, \
                self.rpid, \
                self.idmap, \
                self.record_zhisou_flow, \
                self.gen_log_ip, \
                self.info_from_ip = self.loglist



                self.class_info = JudgmentBusiness.judgment_business(self.business)
                self.business_level1 = 'UNKNOW' if self.class_info[0] is None else self.class_info[0]
                self.business_level2 = 'UNKNOW' if self.class_info[0] is None else self.class_info[1]
                self.business_level3 = 'UNKNOW' if self.class_info[0] is None else self.class_info[2]

                self.adid_count = self.statistical_pv(self.adid)                
                self.taobao_accountid_count = self.statistical_pv_taobao(self.accountid, self.msg_count)                
                self.tmp = self.judgment_taobao_channel(self.pid, self.msg_count)                
                self.taobao_channel = 'UNKNOW' if self.tmp is None else self.tmp

                if self.more_values_count >= self.write_msg_to_db_line:
                    self.clickhouse.execute("insert into ws_ie_log_test (time, \
                                                                         optime, \
                                                                         requestip, \
                                                                         pid, \
                                                                         suyy_id, \
                                                                         groupid, \
                                                                         adid, \
                                                                         accountid, \
                                                                         flag, \
                                                                         reserved, \
                                                                         keyword, \
                                                                         cost, \
                                                                         refer, \
                                                                         inquire_word, \
                                                                         transip, \
                                                                         uuid, \
                                                                         rerank_price, \
                                                                         query_reserved, \
                                                                         pvid, \
                                                                         rank, \
                                                                         price, \
                                                                         qc_inquire_word, \
                                                                         front_type, \
                                                                         strategy_rerank, \
                                                                         zhisou_flag, \
                                                                         all_cost, \
                                                                         health, \
                                                                         p, \
                                                                         w, \
                                                                         none, \
                                                                         business_code, \
                                                                         original_ctr, \
                                                                         area_obtaining_method, \
                                                                         pv_tags, \
                                                                         ad_type, \
                                                                         return_content_length, \
                                                                         front_ua, \
                                                                         flow_flag, \
                                                                         retrieve_flag, \
                                                                         start_price, \
                                                                         keyword_business, \
                                                                         inquire_word_type, \
                                                                         front_channel_number, \
                                                                         front_domain_type, \
                                                                         return_dynamic_subchain_id, \
                                                                         city_region, \
                                                                         pno, \
                                                                         requestfile, \
                                                                         candidate_ad, \
                                                                         baichuan_pvtype, \
                                                                         mfp, \
                                                                         msesuuid, \
                                                                         style_reserve, \
                                                                         cookie_info, \
                                                                         chn, \
                                                                         guid, \
                                                                         is_https UInt8, \
                                                                         teston UInt8, \
                                                                         puuid, \
                                                                         convertSogoufree, \
                                                                         finally_ctr, \
                                                                         credibility, \
                                                                         miaf , \
                                                                         lbs_return_city_code, \
                                                                         below_ad, \
                                                                         galaxy_return_flag, \
                                                                         is_in_service_type_file UInt8, \
                                                                         dp_rc, \
                                                                         web_api_search, \
                                                                         sogouhao_pass_uuid, \
                                                                         sogouhao_pass_siteid, \
                                                                         sogouhao_pass_ad_place, \
                                                                         is_logo UInt8, \
                                                                         jinshu_articleid, \
                                                                         public_articleid, \
                                                                         rpid, \
                                                                         idmap, \
                                                                         record_zhisou_flow, \
                                                                         business_level1, \
                                                                         business_level2, \
                                                                         business_level3, \
                                                                         adid_count UInt8, \
                                                                         taobao_accountid_count UInt8, \
                                                                         taobao_channel, \
                                                                         gen_log_ip, \
                                                                         info_from_ip) values", self.more_values)
                    self.more_values_count = 0
                    self.more_values = []
                else:
                    
                    self.line_values = [self.time,
                                        self.optime,
                                        self.requestip,
                                        self.pid,
                                        self.suyy_id,
                                        self.groupid,
                                        self.adid,
                                        self.accountid,
                                        self.flag,
                                        self.reserved,
                                        self.keyword,
                                        self.cost,
                                        self.refer,
                                        self.inquire_word,
                                        self.transip,
                                        self.uuid,
                                        self.rerank_price,
                                        self.query_reserved,
                                        self.pvid,
                                        self.rank,
                                        self.price,
                                        self.qc_inquire_word,
                                        self.front_type,
                                        self.strategy_rerank,
                                        self.zhisou_flag,
                                        self.all_cost,
                                        self.health,
                                        self.p,
                                        self.w,
                                        self.none,
                                        self.business_code,
                                        self.original_ctr,
                                        self.area_obtaining_method,
                                        self.pv_tags,
                                        self.ad_type,
                                        self.return_content_length,
                                        self.front_ua,
                                        self.flow_flag,
                                        self.retrieve_flag,
                                        self.start_price,
                                        self.keyword_business,
                                        self.inquire_word_type,
                                        self.front_channel_number,
                                        self.front_domain_type,
                                        self.return_dynamic_subchain_id,
                                        self.city_region,
                                        self.pno,
                                        self.requestfile,
                                        self.candidate_ad,
                                        self.baichuan_pvtype,
                                        self.mfp,
                                        self.msesuuid,
                                        self.style_reserve,
                                        self.cookie_info,
                                        self.chn,
                                        self.guid,
                                        self.is_https,
                                        self.teston,
                                        self.puuid,
                                        self.convertSogoufree,
                                        self.finally_ctr,
                                        self.credibility,
                                        self.miaf,
                                        self.lbs_return_city_code,
                                        self.below_ad,
                                        self.galaxy_return_flag,
                                        self.is_in_service_type_file,
                                        self.dp_rc,
                                        self.web_api_search,
                                        self.sogouhao_pass_uuid,
                                        self.sogouhao_pass_siteid,
                                        self.sogouhao_pass_ad_place,
                                        self.is_logo,
                                        self.jinshu_articleid,
                                        self.public_articleid,
                                        self.rpid,
                                        self.idmap,
                                        self.record_zhisou_flow,
                                        self.business_level1,
                                        self.business_level2,
                                        self.business_level3,
                                        self.adid_count,
                                        self.taobao_accountid_count,
                                        self.taobao_channel,
                                        self.gen_log_ip,
                                        self.info_from_ip]
                    self.more_values.append(self.line_values)
                    self.more_values_count += 1

