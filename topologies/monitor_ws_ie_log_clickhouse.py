"""
Word count topology
"""

from streamparse import Grouping, Topology

from spouts.clickhouse_src_ws_ie_log_spout import WsIeLogSpout
from bolts.clickhouse_reliable_wsielog_tag import FilterLogClickHouseBolt
from bolts.clickhouse_reliable_wsielog_business import WsIeLogClickHouseBolt



class AnaylzerWsIeLog(Topology):

    wsielog_clickhouse_spout = WsIeLogSpout.spec(par=3)
    wsielog_clickhouse_tag_bolt = FilterLogClickHouseBolt.spec(inputs=[wsielog_clickhouse_spout['wsielog_spout']],par=3)
    wsielog_clickhouse_entry_ck = WsIeLogClickHouseBolt.spec(inputs=[wsielog_clickhouse_tag_bolt['clickhouse_tag_wsielog_stream']], par=40)


