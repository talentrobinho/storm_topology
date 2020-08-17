"""
Word count topology
"""

from streamparse import Grouping, Topology

from spouts.click_log_spout import CdIeLogSpout, BillIeLogSpout
from bolts.filter_click import FilterClickBolt
from bolts.click_report import TagClickBolt


class MonitorClick(Topology):
    click_spout = BillIeLogSpout.spec()
    filter_click_bolt = FilterClickBolt.spec(inputs=[click_spout],par=3)
    tag_click = TagClickBolt.spec(inputs=[filter_click_bolt['filter_click_stream']], par=3)
