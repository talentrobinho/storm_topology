"""
Word count topology
"""

from streamparse import Grouping, Topology

from spouts.ws_ie_log_spout import WSIeLogSpout
from bolts.filter_ws_log import FilterWSLogBolt
from bolts.analysis_ws_log_clickhouse import *


class AnalysisWSLog(Topology):
    ws_log_spout = WSIeLogSpout.spec(par=6)
    ws_log_stream_bolt = FilterWSLogBolt.spec(inputs=[ws_log_spout],par=6)
    '''taobao calculate consume'''
    tag_ws_log_bolt = TagWSLogBolt.spec(inputs=[ws_log_stream_bolt['filter_ws_log_stream']], par=18)

