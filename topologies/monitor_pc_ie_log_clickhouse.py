"""
Word count topology
"""

from streamparse import Grouping, Topology

from spouts.clickhouse_src_pc_ie_log_spout import PCIeLogSpout
from bolts.clickhouse_reliable_pcielog_tag import FilterLogClickHouseBolt
from bolts.clickhouse_reliable_pcielog_business import PCIeLogClickHouseBolt



class AnaylzerPCIeLog(Topology):

    pcielog_clickhouse_spout = PCIeLogSpout.spec(par=3)
    pcielog_clickhouse_tag_bolt = FilterLogClickHouseBolt.spec(inputs=[pcielog_clickhouse_spout['pcielog_spout']],par=3)
    pcielog_clickhouse_entry_ck = PCIeLogClickHouseBolt.spec(inputs=[pcielog_clickhouse_tag_bolt['clickhouse_tag_pcielog_stream']], par=20)


