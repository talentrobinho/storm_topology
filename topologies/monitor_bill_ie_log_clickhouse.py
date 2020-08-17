"""
Word count topology
"""

from streamparse import Grouping, Topology

from spouts.clickhouse_src_bill_ie_log_spout import BillIeLogSpout
from bolts.clickhouse_reliable_billielog_tag import FilterLogClickHouseBolt
from bolts.clickhouse_reliable_billielog_business import BillIeLogClickHouseBolt



class AnaylzerBillIeLog(Topology):

    billielog_clickhouse_spout = BillIeLogSpout.spec(par=3)
    billielog_clickhouse_tag_bolt = FilterLogClickHouseBolt.spec(inputs=[billielog_clickhouse_spout['billielog_spout']],par=3)
    billielog_clickhouse_entry_ck = BillIeLogClickHouseBolt.spec(inputs=[billielog_clickhouse_tag_bolt['clickhouse_tag_billielog_stream']], par=6)


