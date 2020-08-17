"""
Word count topology
"""

from streamparse import Grouping, Topology

from spouts.clickhouse_reliable_consume_src_cdielog_spout import CdIeLogReliableSpout
from spouts.clickhouse_consume_src_cdielog_spout import CdIeLogSpout
from bolts.clickhouse_reliable_consume_tag_cdielog import FilterLogClickHouseBolt
from bolts.clickhouse_reliable_consume_business import ConsumeClickHouseBolt



class CalculateConsume(Topology):
    #consume_clickhouse_spout = CdIeLogReliableSpout.spec(par=1)
    #consume_clickhouse_tag_bolt = FilterLogClickHouseBolt.spec(inputs=[consume_clickhouse_spout['cdielog_reliable_spout']],par=3)
    #consume_clickhouse_entry = ConsumeClickHouseBolt.spec(inputs=[consume_clickhouse_tag_bolt['clickhouse_tag_cdielog_stream']], par=3)


    consume_clickhouse_spout = CdIeLogSpout.spec(par=2)
    consume_clickhouse_tag_bolt = FilterLogClickHouseBolt.spec(inputs=[consume_clickhouse_spout['cdielog_spout']],par=3)
    consume_clickhouse_entry = ConsumeClickHouseBolt.spec(inputs=[consume_clickhouse_tag_bolt['clickhouse_tag_cdielog_stream']], par=3)


