"""
Word count topology
"""

from streamparse import Grouping, Topology

from spouts.cd_ie_log_guangguang_spout import CdIeLogInputGuangSpout
from bolts.filter_log_guangguang import FilterLogInputGuangBolt
from bolts.filter_log_guangguang_channel import *


class CalculateInputGuangConsume(Topology):
    input_guang_consume_spout = CdIeLogInputGuangSpout.spec(par=3)
    input_guang_stream_bolt = FilterLogInputGuangBolt.spec(inputs=[input_guang_consume_spout['input_guang_spout']],par=3)
    '''input calculate consume'''
    #input_chat_consume_topol = InputChatFilterLogBolt.spec(inputs=[input_sub_stream_bolt['filter_input_log_stream']], par=2)
    #input_direct_consume_topol = InputDirectFilterLogBolt.spec(inputs=[input_sub_stream_bolt['filter_input_log_stream']], par=2)
    #input_recomm_consume_topol = InputRecommFilterLogBolt.spec(inputs=[input_sub_stream_bolt['filter_input_log_stream']], par=1)
    input_guang_consume_topol = InputGuangGuangFilterLogBolt.spec(inputs=[input_guang_stream_bolt['filter_input_guang_log_stream']], par=1)

