"""
Word count topology
"""

from streamparse import Grouping, Topology

from spouts.cd_ie_log_spout import CdIeLogSpout
from bolts.filter_log import FilterLogBolt
from bolts.calculate_taobao_consume import *


class CalculateTaobaoConsume(Topology):
    taobao_consume_spout = CdIeLogSpout.spec()
    taobao_consume_filter_bolt = FilterLogBolt.spec(inputs=[taobao_consume_spout],par=3)
    '''taobao calculate consume'''
    taobao_consume_bolt = TAOBAOTotalBolt.spec(inputs=[taobao_consume_filter_bolt['filter_log_stream']], par=1)

    ### '''pc channel stream'''
    ### #sub_pc_sohu_stream_bolt = SohuFilterLogBolt.spec(inputs={sub_stream_bolt['filter_log_stream']: Grouping.fields('timestamp')}, par=1)
    ### #sub_pc_sogou_stream_bolt = SogouFilterLogBolt.spec(inputs={sub_stream_bolt['filter_log_stream']: Grouping.fields('timestamp')}, par=1)
    ### #sub_pc_soso_stream_bolt = SosoFilterLogBolt.spec(inputs={sub_stream_bolt['filter_log_stream']: Grouping.fields('timestamp')}, par=1)
    ### #sub_pc_123_stream_bolt = OneTwoThreeFilterLogBolt.spec(inputs={sub_stream_bolt['filter_log_stream']: Grouping.fields('timestamp')}, par=1)
    ### #sub_pc_bd_stream_bolt = BDFilterLogBolt.spec(inputs={sub_stream_bolt['filter_log_stream']: Grouping.fields('timestamp')}, par=1)
    ### #sub_pc_bc_stream_bolt = BDFilterLogBolt.spec(inputs={sub_stream_bolt['filter_log_stream']: Grouping.fields('timestamp')}, par=1)
    ### #sub_pc_brws_stream_bolt = BrwsFilterLogBolt.spec(inputs={sub_stream_bolt['filter_log_stream']: Grouping.fields('timestamp')}, par=1)
    ### #sub_pc_ime_stream_bolt = ImeFilterLogBolt.spec(inputs={sub_stream_bolt['filter_log_stream']: Grouping.fields('timestamp')}, par=1)
    ### sub_pc_sohu_stream_bolt = SohuFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)
    ### sub_pc_sogou_stream_bolt = SogouFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)
    ### sub_pc_soso_stream_bolt = SOSOFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)
    ### sub_pc_123_stream_bolt = OneTwoThreeFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)
    ### sub_pc_bd_stream_bolt = BDFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)
    ### sub_pc_bc_stream_bolt = BCFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)
    ### sub_pc_brws_stream_bolt = BrwsFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)
    ### sub_pc_ime_stream_bolt = IMEFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)

    ### '''pc calculate consume'''
    ### #consume_bolt = ConsumeBolt.spec(inputs=[sub_stream_bolt['pc_bidding_total']], par=1)
    ### #pc_consume_bolt = PCConsumeBolt.spec(inputs={sub_stream_bolt['pc_bidding_total_stream']: Grouping.fields('consume')}, par=1)
    ### pc_consume_bolt = PCConsumeBolt.spec(inputs=[sub_pc_sohu_stream_bolt['pc_bidding_total_stream'],
    ###                                              sub_pc_sogou_stream_bolt['pc_bidding_total_stream'],
    ###                                              sub_pc_soso_stream_bolt['pc_bidding_total_stream'],
    ###                                              sub_pc_123_stream_bolt['pc_bidding_total_stream'],
    ###                                              sub_pc_bd_stream_bolt['pc_bidding_total_stream'],
    ###                                              sub_pc_bc_stream_bolt['pc_bidding_total_stream'],
    ###                                              sub_pc_brws_stream_bolt['pc_bidding_total_stream'],
    ###                                              sub_pc_ime_stream_bolt['pc_bidding_total_stream']],
    ###                                      par=1)
    ### pc_sohu_consume_bolt = PCSOHUConsumeBolt.spec(inputs=[sub_pc_sohu_stream_bolt['pc_bidding_sohu_stream']], par=1)
    ### pc_sogou_consume_bolt = PCSOGOUConsumeBolt.spec(inputs=[sub_pc_sogou_stream_bolt['pc_bidding_sogou_stream']], par=1)
    ### pc_soso_consume_bolt = PCSOSOConsumeBolt.spec(inputs=[sub_pc_soso_stream_bolt['pc_bidding_soso_stream']], par=1)
    ### pc_123_consume_bolt = PCOTTConsumeBolt.spec(inputs=[sub_pc_123_stream_bolt['pc_bidding_123_stream']], par=1)
    ### pc_bd_consume_bolt = PCBDConsumeBolt.spec(inputs=[sub_pc_bd_stream_bolt['pc_bidding_bd_stream']], par=1)
    ### pc_bc_consume_bolt = PCBCConsumeBolt.spec(inputs=[sub_pc_bc_stream_bolt['pc_bidding_bc_stream']], par=1)
    ### pc_brws_consume_bolt = PCBRWSConsumeBolt.spec(inputs=[sub_pc_brws_stream_bolt['pc_bidding_brws_stream']], par=1)
    ### pc_ime_consume_bolt = PCIMEConsumeBolt.spec(inputs=[sub_pc_ime_stream_bolt['pc_bidding_ime_stream']], par=1)

    ### #batching# pc_consume_bolt = PCConsumeBatchingBolt.spec(inputs=[sub_pc_sohu_stream_bolt['pc_bidding_total_stream'],
    ### #batching#                                              sub_pc_sogou_stream_bolt['pc_bidding_total_stream'],
    ### #batching#                                              sub_pc_soso_stream_bolt['pc_bidding_total_stream'],
    ### #batching#                                              sub_pc_123_stream_bolt['pc_bidding_total_stream'],
    ### #batching#                                              sub_pc_bd_stream_bolt['pc_bidding_total_stream'],
    ### #batching#                                              sub_pc_bc_stream_bolt['pc_bidding_total_stream'],
    ### #batching#                                              sub_pc_brws_stream_bolt['pc_bidding_total_stream'],
    ### #batching#                                              sub_pc_ime_stream_bolt['pc_bidding_total_stream']],
    ### #batching#                                      par=1)

    ### #batching# pc_sohu_consume_bolt = PCSOHUConsumeBatchingBolt.spec(inputs=[sub_pc_sohu_stream_bolt['pc_bidding_sohu_stream']], par=1)
    ### #batching# pc_sogou_consume_bolt = PCSOGOUConsumeBatchingBolt.spec(inputs=[sub_pc_sogou_stream_bolt['pc_bidding_sogou_stream']], par=1)
    ### #batching# pc_soso_consume_bolt = PCSOSOConsumeBatchingBolt.spec(inputs=[sub_pc_soso_stream_bolt['pc_bidding_soso_stream']], par=1)
    ### #batching# pc_123_consume_bolt = PCOTTConsumeBatchingBolt.spec(inputs=[sub_pc_123_stream_bolt['pc_bidding_123_stream']], par=1)
    ### #batching# pc_bd_consume_bolt = PCBDConsumeBatchingBolt.spec(inputs=[sub_pc_bd_stream_bolt['pc_bidding_bd_stream']], par=1)
    ### #batching# pc_bc_consume_bolt = PCBCConsumeBatchingBolt.spec(inputs=[sub_pc_bc_stream_bolt['pc_bidding_bc_stream']], par=1)
    ### #batching# pc_brws_consume_bolt = PCBRWSConsumeBatchingBolt.spec(inputs=[sub_pc_brws_stream_bolt['pc_bidding_brws_stream']], par=1)
    ### #batching# pc_ime_consume_bolt = PCIMEConsumeBatchingBolt.spec(inputs=[sub_pc_ime_stream_bolt['pc_bidding_ime_stream']], par=1)



    ### '''ws channel stream'''
    ### #sub_ws_qq_stream_bolt = WSQQFilterLogBolt.spec(inputs={sub_stream_bolt['filter_log_stream']: Grouping.fields('timestamp')}, par=1)
    ### #sub_ws_sogou_stream_bolt = WSSogouFilterLogBolt.spec(inputs={sub_stream_bolt['filter_log_stream']: Grouping.fields('timestamp')}, par=1)
    ### #sub_ws_waigou_stream_bolt = WSWaigouFilterLogBolt.spec(inputs={sub_stream_bolt['filter_log_stream']: Grouping.fields('timestamp')}, par=1)
    ### sub_ws_qq_stream_bolt = WSQQFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)
    ### sub_ws_sogou_stream_bolt = WSSogouFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)
    ### sub_ws_waigou_stream_bolt = WSWaigouFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)

    ### '''ws calculate consume'''
    ### ###ws_consume_bolt = WSConsumeBolt.spec(inputs={sub_stream_bolt['ws_bidding_total']: Grouping.fields('consume')}, par=1)
    ### ###ws_consume_bolt = WSConsumeBolt.spec(inputs={sub_ws_qq_stream_bolt['ws_bidding_total_stream']: Grouping.fields('consume'),
    ### ###                                             sub_ws_sogou_stream_bolt['ws_bidding_total_stream']: Grouping.fields('consume'),
    ### ###                                             sub_ws_waigou_stream_bolt['ws_bidding_total_stream']: Grouping.fields('consume')}, 
    ### ###                                     par=1)

    ### ws_consume_bolt = WSConsumeBolt.spec(inputs=[sub_ws_qq_stream_bolt['ws_bidding_total_stream'],
    ###                                              sub_ws_sogou_stream_bolt['ws_bidding_total_stream'],
    ###                                              sub_ws_waigou_stream_bolt['ws_bidding_total_stream']], 
    ###                                      par=3)
    ### ws_qq_consume_bolt = WSQQConsumeBolt.spec(inputs=[sub_ws_qq_stream_bolt['ws_bidding_qq_stream']],par=1)
    ### ws_sogou_consume_bolt = WSSOGOUConsumeBolt.spec(inputs=[sub_ws_sogou_stream_bolt['ws_bidding_sogou_stream']],par=1)
    ### ws_waigou_consume_bolt = WSWAIGOUConsumeBolt.spec(inputs=[sub_ws_waigou_stream_bolt['ws_bidding_waigou_stream']],par=1)


    ### #batching# ws_consume_bolt = WSConsumeBatchingBolt.spec(inputs=[sub_ws_qq_stream_bolt['ws_bidding_total_stream'],
    ### #batching#                                              sub_ws_sogou_stream_bolt['ws_bidding_total_stream'],
    ### #batching#                                              sub_ws_waigou_stream_bolt['ws_bidding_total_stream']], 
    ### #batching#                                      par=1)
    ### #batching# ws_qq_consume_bolt = WSQQConsumeBatchingBolt.spec(inputs=[sub_ws_qq_stream_bolt['ws_bidding_qq_stream']],par=1)
    ### #batching# ws_sogou_consume_bolt = WSSOGOUConsumeBatchingBolt.spec(inputs=[sub_ws_sogou_stream_bolt['ws_bidding_sogou_stream']],par=1)
    ### #batching# ws_waigou_consume_bolt = WSWAIGOUConsumeBatchingBolt.spec(inputs=[sub_ws_waigou_stream_bolt['ws_bidding_waigou_stream']],par=1)
