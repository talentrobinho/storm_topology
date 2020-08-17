"""
Word count topology
"""

from streamparse import Grouping, Topology

from spouts.cd_ie_log_spout import CdIeLogSpout
from bolts.filter_log import FilterLogBolt
from bolts.filter_log_pc_channel import *
from bolts.filter_log_ws_channel import *
from bolts.calculate_consume import *
from bolts.calculate_taobao_consume import *


class CalculateConsume(Topology):
    consume_spout = CdIeLogSpout.spec(par=3)
    #sub_stream_bolt = FilterLogBolt.spec(inputs={consume_spout: Grouping.fields('cd_ie_log')},par=3)
    #sub_stream_bolt = FilterLogBolt.spec(inputs={consume_spout: Grouping.fields('cd_ie_log')},par=18)
    sub_stream_bolt = FilterLogBolt.spec(inputs=[consume_spout['search_spout']],par=3)


    '''taobao calculate consume'''
    #taobao_consume_bolt = TAOBAOTotalBolt.spec(inputs=[taobao_sub_stream_bolt['filter_log_stream']], par=1)
    #taobao_consume_bolt = TAOBAOTotalBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=4)

    '''ws channel stream'''
    #sub_ws_qq_stream_bolt = WSQQFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)
    #sub_ws_sogou_stream_bolt = WSSogouFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)
    #sub_ws_waigou_stream_bolt = WSWaigouFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=3)
    ws_qq_consume = WSQQFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)
    ws_sogou_consume = WSSogouFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)
    ws_waigou_consume = WSWaigouFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=2)

    '''pc channel stream'''
    #sub_pc_sohu_stream_bolt = SohuFilterLogBolt.spec(inputs={sub_stream_bolt['filter_log_stream']: Grouping.fields('timestamp')}, par=1)
    #sub_pc_sogou_stream_bolt = SogouFilterLogBolt.spec(inputs={sub_stream_bolt['filter_log_stream']: Grouping.fields('timestamp')}, par=1)
    #sub_pc_soso_stream_bolt = SosoFilterLogBolt.spec(inputs={sub_stream_bolt['filter_log_stream']: Grouping.fields('timestamp')}, par=1)
    #sub_pc_123_stream_bolt = OneTwoThreeFilterLogBolt.spec(inputs={sub_stream_bolt['filter_log_stream']: Grouping.fields('timestamp')}, par=1)
    #sub_pc_bd_stream_bolt = BDFilterLogBolt.spec(inputs={sub_stream_bolt['filter_log_stream']: Grouping.fields('timestamp')}, par=1)
    #sub_pc_bc_stream_bolt = BDFilterLogBolt.spec(inputs={sub_stream_bolt['filter_log_stream']: Grouping.fields('timestamp')}, par=1)
    #sub_pc_brws_stream_bolt = BrwsFilterLogBolt.spec(inputs={sub_stream_bolt['filter_log_stream']: Grouping.fields('timestamp')}, par=1)
    #sub_pc_ime_stream_bolt = ImeFilterLogBolt.spec(inputs={sub_stream_bolt['filter_log_stream']: Grouping.fields('timestamp')}, par=1)
    sub_pc_sohu_stream_bolt = SohuFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)
    sub_pc_sogou_stream_bolt = SogouFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)
    sub_pc_soso_stream_bolt = SOSOFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)
    sub_pc_123_stream_bolt = OneTwoThreeFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)
    sub_pc_bd_stream_bolt = BDFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)
    sub_pc_bc_stream_bolt = BCFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)
    sub_pc_brws_stream_bolt = BrwsFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)
    sub_pc_ime_stream_bolt = IMEFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)

    '''pc calculate consume'''
    #consume_bolt = ConsumeBolt.spec(inputs=[sub_stream_bolt['pc_bidding_total']], par=1)
    #pc_consume_bolt = PCConsumeBolt.spec(inputs={sub_stream_bolt['pc_bidding_total_stream']: Grouping.fields('consume')}, par=1)
    #pc_consume_bolt = PCConsumeBolt.spec(inputs=[sub_pc_sohu_stream_bolt['pc_bidding_total_stream'],
    #                                             sub_pc_sogou_stream_bolt['pc_bidding_total_stream'],
    #                                             sub_pc_soso_stream_bolt['pc_bidding_total_stream'],
    #                                             sub_pc_123_stream_bolt['pc_bidding_total_stream'],
    #                                             sub_pc_bd_stream_bolt['pc_bidding_total_stream'],
    #                                             sub_pc_bc_stream_bolt['pc_bidding_total_stream'],
    #                                             sub_pc_brws_stream_bolt['pc_bidding_total_stream'],
    #                                             sub_pc_ime_stream_bolt['pc_bidding_total_stream']],
    #                                     par=1)
    pc_sohu_consume= PCSOHUConsumeBolt.spec(inputs=[sub_pc_sohu_stream_bolt['pc_bidding_sohu_stream']], par=1)
    pc_sogou_consume= PCSOGOUConsumeBolt.spec(inputs=[sub_pc_sogou_stream_bolt['pc_bidding_sogou_stream']], par=1)
    pc_soso_consume= PCSOSOConsumeBolt.spec(inputs=[sub_pc_soso_stream_bolt['pc_bidding_soso_stream']], par=1)
    pc_123_consume= PCOTTConsumeBolt.spec(inputs=[sub_pc_123_stream_bolt['pc_bidding_123_stream']], par=1)
    pc_bd_consume= PCBDConsumeBolt.spec(inputs=[sub_pc_bd_stream_bolt['pc_bidding_bd_stream']], par=1)
    pc_bc_consume= PCBCConsumeBolt.spec(inputs=[sub_pc_bc_stream_bolt['pc_bidding_bc_stream']], par=1)
    pc_brws_consume= PCBRWSConsumeBolt.spec(inputs=[sub_pc_brws_stream_bolt['pc_bidding_brws_stream']], par=1)
    pc_ime_consume= PCIMEConsumeBolt.spec(inputs=[sub_pc_ime_stream_bolt['pc_bidding_ime_stream']], par=1)

    #batching# pc_consume_bolt = PCConsumeBatchingBolt.spec(inputs=[sub_pc_sohu_stream_bolt['pc_bidding_total_stream'],
    #batching#                                              sub_pc_sogou_stream_bolt['pc_bidding_total_stream'],
    #batching#                                              sub_pc_soso_stream_bolt['pc_bidding_total_stream'],
    #batching#                                              sub_pc_123_stream_bolt['pc_bidding_total_stream'],
    #batching#                                              sub_pc_bd_stream_bolt['pc_bidding_total_stream'],
    #batching#                                              sub_pc_bc_stream_bolt['pc_bidding_total_stream'],
    #batching#                                              sub_pc_brws_stream_bolt['pc_bidding_total_stream'],
    #batching#                                              sub_pc_ime_stream_bolt['pc_bidding_total_stream']],
    #batching#                                      par=1)

    #batching# pc_sohu_consume_bolt = PCSOHUConsumeBatchingBolt.spec(inputs=[sub_pc_sohu_stream_bolt['pc_bidding_sohu_stream']], par=1)
    #batching# pc_sogou_consume_bolt = PCSOGOUConsumeBatchingBolt.spec(inputs=[sub_pc_sogou_stream_bolt['pc_bidding_sogou_stream']], par=1)
    #batching# pc_soso_consume_bolt = PCSOSOConsumeBatchingBolt.spec(inputs=[sub_pc_soso_stream_bolt['pc_bidding_soso_stream']], par=1)
    #batching# pc_123_consume_bolt = PCOTTConsumeBatchingBolt.spec(inputs=[sub_pc_123_stream_bolt['pc_bidding_123_stream']], par=1)
    #batching# pc_bd_consume_bolt = PCBDConsumeBatchingBolt.spec(inputs=[sub_pc_bd_stream_bolt['pc_bidding_bd_stream']], par=1)
    #batching# pc_bc_consume_bolt = PCBCConsumeBatchingBolt.spec(inputs=[sub_pc_bc_stream_bolt['pc_bidding_bc_stream']], par=1)
    #batching# pc_brws_consume_bolt = PCBRWSConsumeBatchingBolt.spec(inputs=[sub_pc_brws_stream_bolt['pc_bidding_brws_stream']], par=1)
    #batching# pc_ime_consume_bolt = PCIMEConsumeBatchingBolt.spec(inputs=[sub_pc_ime_stream_bolt['pc_bidding_ime_stream']], par=1)



