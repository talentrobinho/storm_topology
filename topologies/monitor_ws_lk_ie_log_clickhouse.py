"""
Word count topology
"""

from streamparse import Grouping, Topology

from spouts.clickhouse_src_ws_lk_ie_log_spout import WsLKIeLogSpout
from bolts.clickhouse_reliable_wslkielog_tag import FilterLogClickHouseBolt
from bolts.clickhouse_reliable_wslkielog_business import WsLKIeLogClickHouseBolt



class AnaylzerWsLKIeLog(Topology):

    wslkielog_clickhouse_spout = WsLKIeLogSpout.spec(par=3)
    wslkielog_clickhouse_tag_bolt = FilterLogClickHouseBolt.spec(inputs=[wslkielog_clickhouse_spout['wslkielog_spout']],par=3)
    wslkielog_clickhouse_entry_ck = WsLKIeLogClickHouseBolt.spec(inputs=[wslkielog_clickhouse_tag_bolt['clickhouse_tag_wslkielog_stream']], par=10)


