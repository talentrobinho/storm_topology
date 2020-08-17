"""
Word count topology
"""

from streamparse import Grouping, Topology

from spouts.cd_ie_log_input_spout import CdIeLogInputSpout
from bolts.filter_log_input import FilterLogInputBolt
from bolts.filter_log_input_channel import *


class CalculateInputConsume(Topology):
    input_consume_spout = CdIeLogInputSpout.spec(par=1)
    input_sub_stream_bolt = FilterLogInputBolt.spec(inputs=[input_consume_spout['input_spout']],par=3)
    '''input calculate consume'''
    input_chat_consume_topol = InputChatFilterLogBolt.spec(inputs=[input_sub_stream_bolt['filter_input_log_stream']], par=1)
    input_direct_consume_topol = InputDirectFilterLogBolt.spec(inputs=[input_sub_stream_bolt['filter_input_log_stream']], par=1)
    input_recomm_consume_topol = InputRecommFilterLogBolt.spec(inputs=[input_sub_stream_bolt['filter_input_log_stream']], par=1)
    #input_guangguang_consume_topol = InputGuangGuangFilterLogBolt.spec(inputs=[input_sub_stream_bolt['filter_input_log_stream']], par=1)

