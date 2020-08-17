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


class WsIeLogClickHouseBolt(Bolt):
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
        self.write_msg_to_db_line   = 500

        self.conndb     = ConnDB()
        self.clickhouse = self.conndb.conn_clickhouse_cluster()
        self.redis      = self.conndb.conn_redis()
        #self.redis      = self.conndb.conn_bak_redis()

    def process(self, tup):
        self.loglist        = tup.values
        self.tupid          = tup.id
        self.sec            = str(int(time.time()))

        self.pid            = self.loglist[2]
        self.ad_number      = 0 if self.loglist[5] == '' else len(self.loglist[5].split(','))
        self.is_ad_display  = 0 if self.loglist[5] == '' else 1

        #self.logger.info("timestamp [check data in redis]: {}".format(int(round(time.time() * 1000000))))
        #if self.redis.get(self.tupid) is None:
            #self.logger.info("timestamp [insert into redis start]: {}".format(int(round(time.time() * 1000000))))
        if self.redis.set(self.tupid, 1, ex=600, nx=True):
            #self.logger.info("timestamp [insert into redis fininsh]: {}".format(int(round(time.time() * 1000000))))
            if self.more_values_count >= self.write_msg_to_db_line:
                #self.logger.info("timestamp [insert into ck start]: {}".format(int(round(time.time() * 1000000))))
                self.clickhouse.execute("insert into ws_ie_log_all (time, \
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
                                                              rerank_flag, \
                                                              zhisou_flag, \
                                                              time_consuming, \
                                                              health_rule, \
                                                              p, \
                                                              w, \
                                                              material_triple, \
                                                              business, \
                                                              original_ctr, \
                                                              ext_reserve, \
                                                              creativeid, \
                                                              billing_price, \
                                                              quality, \
                                                              offer_price, \
                                                              planid, \
                                                              get_region_way, \
                                                              pv_tags, \
                                                              ad_style, \
                                                              return_len, \
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
                                                              request_file, \
                                                              ad_candidate_style, \
                                                              match_type, \
                                                              mfp_base64decode, \
                                                              msesuuid, \
                                                              style_reserve, \
                                                              cookie_info, \
                                                              chn, \
                                                              guid, \
                                                              is_https, \
                                                              teston, \
                                                              puuid, \
                                                              convert_sogou_free, \
                                                              final_ctr, \
                                                              credibility, \
                                                              miaf, \
                                                              city_code, \
                                                              is_belowad_frontcode, \
                                                              galaxy_flag, \
                                                              pid_in_service_type_file, \
                                                              dp_and_rc, \
                                                              web_api_search, \
                                                              fast_deliver_id, \
                                                              sogouhao_pass_uuid, \
                                                              sogouhao_pass_siteid, \
                                                              sogouhao_pass_adposition, \
                                                              is_logo, \
                                                              artid_gid, \
                                                              huazhang_ad, \
                                                              rpid, \
                                                              idmap, \
                                                              zhisou_type, \
                                                              bigsearch_test, \
                                                              plan_type, \
                                                              gen_log_ip, \
                                                              optime, \
                                                              ad_number, \
                                                              is_ad_display) values", self.more_values)
                self.more_values_count  = 0
                self.more_values        = []
                #self.logger.info("timestamp [insert into ck fininsh]: {}".format(int(round(time.time() * 1000000))))
            else:
                
                self.other_values = (self.sec, 
                                     self.ad_number, 
                                     self.is_ad_display
                                    )

                self.line_values = self.loglist + self.other_values
                self.more_values.append(self.line_values)
                self.more_values_count += 1


