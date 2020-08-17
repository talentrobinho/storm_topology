"""
Word count topology
"""

from streamparse import Grouping, Topology

from spouts.click_log_spout import CdIeLogSpout, BillIeLogSpout
from bolts.taobao_back_imei import FilterBackTypeBolt


class MonitorBackType(Topology):
    taobao_back_type_spout = BillIeLogSpout.spec()
    taobao_back_type = FilterBackTypeBolt.spec(inputs=[taobao_back_type_spout],par=3)
