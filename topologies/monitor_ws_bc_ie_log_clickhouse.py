"""
Word count topology
"""

from streamparse import Grouping, Topology

from spouts.clickhouse_src_ws_bc_ie_log_spout import WsBCIeLogSpout
from bolts.clickhouse_reliable_wsbcielog_tag import FilterLogClickHouseBolt
from bolts.clickhouse_reliable_wsbcielog_business import WsBCIeLogClickHouseBolt



class AnaylzerWsBCIeLog(Topology):

    wsbcielog_clickhouse_spout = WsBCIeLogSpout.spec(par=1)
    wsbcielog_clickhouse_tag_bolt = FilterLogClickHouseBolt.spec(inputs=[wsbcielog_clickhouse_spout['wsbcielog_spout']],par=1)
    wsbcielog_clickhouse_entry_ck = WsBCIeLogClickHouseBolt.spec(inputs=[wsbcielog_clickhouse_tag_bolt['clickhouse_tag_wsbcielog_stream']], par=2)


