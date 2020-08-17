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


class BillIeLogClickHouseBolt(Bolt):
    #config = {'topology.tick.tuple.freq.secs': 1}
    #ticks_between_batches = 10

    def initialize(self, conf, ctx):
        self.tupid                  = None
        self.sec                    = None
        self.redis                  = None


        self.more_values_count      = 0
        self.more_values            = []
        self.write_msg_to_db_line   = 100

        self.conndb     = ConnDB()
        self.clickhouse = self.conndb.conn_clickhouse_cluster()
        self.redis      = self.conndb.conn_redis()

    def process(self, tup):
        self.loglist        = tup.values
        self.tupid          = tup.id
        self.sec            = str(int(time.time()))

    
        if self.redis.get(self.tupid) is None:
            self.redis.set(self.tupid, 1, ex=600, nx=True)
            if self.more_values_count >= self.write_msg_to_db_line:
                self.clickhouse.execute("insert into bill_ie_log_all (time, \
                                                                      request_ip, \
                                                                      pid, \
                                                                      sohuid_inputid, \
                                                                      groupid, \
                                                                      adid, \
                                                                      accountid, \
                                                                      flag, \
                                                                      reserved, \
                                                                      keyword, \
                                                                      cost, \
                                                                      click_sohu_url, \
                                                                      ret, \
                                                                      search_keyword, \
                                                                      class_type, \
                                                                      pv_refer, \
                                                                      clickid, \
                                                                      customer_url, \
                                                                      time_diff, \
                                                                      ip, \
                                                                      ac_type, \
                                                                      mouse_data, \
                                                                      service_type, \
                                                                      pid_pass_rule, \
                                                                      jump_type, \
                                                                      extend_reserved, \
                                                                      creativeid, \
                                                                      planid, \
                                                                      req_ip_type, \
                                                                      hostname, \
                                                                      ma, \
                                                                      cxtype, \
                                                                      cxids, \
                                                                      lu, \
                                                                      upos, \
                                                                      origin_adid, \
                                                                      ad_price, \
                                                                      query_reserved, \
                                                                      history_price, \
                                                                      max_price, \
                                                                      price, \
                                                                      domain_id, \
                                                                      is_redirect, \
                                                                      upid, \
                                                                      ca_id, \
                                                                      ck_id, \
                                                                      region_public, \
                                                                      ak, \
                                                                      show_ip, \
                                                                      fd, \
                                                                      sd, \
                                                                      cxid, \
                                                                      sohu_url, \
                                                                      bill_url, \
                                                                      traffic_sign, \
                                                                      union_refer_site, \
                                                                      crown_style, \
                                                                      crown_creativeid, \
                                                                      crown_imagid, \
                                                                      city_code, \
                                                                      newid, \
                                                                      group_cateid, \
                                                                      cateid_second, \
                                                                      multi_creative_id, \
                                                                      location_id, \
                                                                      real_price, \
                                                                      price_ratio, \
                                                                      style_flag, \
                                                                      yhext, \
                                                                      show_region, \
                                                                      query_classfy, \
                                                                      imei, \
                                                                      taobao_imei_ret_type, \
                                                                      x_forwarded_for, \
                                                                      android_id, \
                                                                      mfp, \
                                                                      erfs, \
                                                                      anticheat_charge_res, \
                                                                      rpid, \
                                                                      user_agent, \
                                                                      ipv6, \
                                                                      plan_type, \
                                                                      cookie, \
                                                                      gen_log_ip, \
                                                                      optime, \
                                                                      time_after_redis) values", self.more_values)
                self.more_values_count  = 0
                self.more_values        = []
            else:
                self.sec_after_redis = str(int(time.time()))
                self.other_values = (self.sec, 
                                     self.sec_after_redis
                                    )

                self.line_values = self.loglist + self.other_values
                self.more_values.append(self.line_values)
                self.more_values_count += 1


