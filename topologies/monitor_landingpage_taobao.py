"""
Word count topology
"""

from streamparse import Grouping, Topology

from spouts.click_log_spout import BillIeLogSpout
from bolts.filter_taobao_landingpage import FilterTTLPBolt


class CalculateTTLP(Topology):
    taobao_url_spout = BillIeLogSpout.spec()
    taobao_url_stream_bolt = FilterTTLPBolt.spec(inputs=[taobao_url_spout],par=1)
