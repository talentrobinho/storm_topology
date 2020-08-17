"""
Word count topology
"""

from streamparse import Grouping, Topology

from spouts.cd_ie_log_spout import CdIeLogSpout
from bolts.filter_log import FilterLogBolt
from bolts.filter_log_union_channel import *


class CalculateConsume(Topology):
    consume_spout = CdIeLogSpout.spec()
    sub_stream_bolt = FilterLogBolt.spec(inputs=[consume_spout],par=7)


    '''ws channel stream'''
    pu_content_consume = PUContentFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)
    pu_searchkey_consume = PUSearchKeyFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)
    wu_content_consume = WUContentFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)
    wu_searchkey_consume = WUSearchKeyFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)
    app_content_consume = AppContentFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)
    app_searchkey_consume = AppSearchKeyFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)
    union_direct_consume = DirectFilterLogBolt.spec(inputs=[sub_stream_bolt['filter_log_stream']], par=1)
