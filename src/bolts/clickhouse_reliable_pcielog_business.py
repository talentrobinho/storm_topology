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


class PCIeLogClickHouseBolt(Bolt):
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
                self.clickhouse.execute("insert into pc_ie_log_all (time, \
                                                                    ip, \
                                                                    pid, \
                                                                    suid, \
                                                                    groupid, \
                                                                    adid, \
                                                                    accountid, \
                                                                    flag, \
                                                                    reserved, \
                                                                    ad_key, \
                                                                    cost, \
                                                                    refer, \
                                                                    query_words, \
                                                                    front_ip, \
                                                                    uuid, \
                                                                    customer_list, \
                                                                    query_words_status, \
                                                                    pvid, \
                                                                    ad_rank_list, \
                                                                    price, \
                                                                    word, \
                                                                    google_top, \
                                                                    google_below, \
                                                                    google_right, \
                                                                    ota_ec_price_rate_list, \
                                                                    request_google, \
                                                                    p_parameter, \
                                                                    m_parameter, \
                                                                    rele_weight, \
                                                                    business_types, \
                                                                    ad_ctr_list, \
                                                                    extend_reserved, \
                                                                    cid_list, \
                                                                    showprice, \
                                                                    quality_value_list, \
                                                                    maxprice, \
                                                                    planid, \
                                                                    quality_list, \
                                                                    sig, \
                                                                    show_brand, \
                                                                    adr_mid_price, \
                                                                    ext_query_reserver, \
                                                                    retrieve_flag, \
                                                                    requested_ad_space, \
                                                                    qt_sogou, \
                                                                    list_of_lowest_bids, \
                                                                    adr_flag, \
                                                                    keyword_list, \
                                                                    traffic_label, \
                                                                    show_strategy, \
                                                                    nflag, \
                                                                    query_classify_res, \
                                                                    extend_flag, \
                                                                    retry_num, \
                                                                    city_code, \
                                                                    lingxi_channel_info, \
                                                                    style_reserved, \
                                                                    budget_price_list, \
                                                                    shg_price, \
                                                                    price_ratio_list, \
                                                                    new_extend_flag, \
                                                                    game_ret_flag, \
                                                                    game_flag, \
                                                                    remarketing_premium_ratio, \
                                                                    ecad_ret_flag, \
                                                                    otaad_ret_flagota, \
                                                                    ota_style, \
                                                                    dacu_price_rate_list, \
                                                                    tuxiu_info, \
                                                                    quality_through_the_log, \
                                                                    leads_marketing_premium_list, \
                                                                    baichuan_revised_advertisement, \
                                                                    style_optimization, \
                                                                    round_ex_log, \
                                                                    style_ctr_log, \
                                                                    final_style_log, \
                                                                    handler_info_url_ipv6, \
                                                                    loginfo_price_ratio_log, \
                                                                    loginfo_match_type_log, \
                                                                    loginfo_plan_type_log, \
                                                                    handler_info_user_health, \
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


