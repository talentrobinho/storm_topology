"""
Word count topology
"""

from streamparse import Grouping, Topology

from spouts.clickhouse_src_ws_ups_update_log_spout import WsUpdateUpdateLogSpout
from bolts.clickhouse_reliable_wsups_updatelog_tag import FilterLogClickHouseBolt
from bolts.clickhouse_reliable_wsups_updatelog_business import WsUpdateUpdateLogClickHouseBolt



class AnaylzerWsUpdateUpdateLog(Topology):

    wsups_updatelog_clickhouse_spout = WsUpdateUpdateLogSpout.spec(par=7)
    wsups_updatelog_clickhouse_tag_bolt = FilterLogClickHouseBolt.spec(inputs=[wsups_updatelog_clickhouse_spout['ups_updatelog_spout']],par=7)
    wsups_updatelog_clickhouse_entry_ck = WsUpdateUpdateLogClickHouseBolt.spec(inputs=[wsups_updatelog_clickhouse_tag_bolt['clickhouse_tag_wsups_updatelog_stream']], par=20)


