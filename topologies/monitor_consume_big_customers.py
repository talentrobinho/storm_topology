"""
big customers topology
"""

from streamparse import Grouping, Topology

from spouts.cd_ie_log_big_customers_spout import CdIeLogBigCustomerSpout
from bolts.filter_log_big_customer import FilterLogBigCustomerBolt
from bolts.calculate_vivo_oppo_mi_consume import *
from bolts.calculate_taobao_consume import *


class CalculateConsume(Topology):
    big_customer_consume_spout = CdIeLogBigCustomerSpout.spec(par=3)
    #filter_big_customer_log_stream_bolt = FilterLogBigCustomerBolt.spec(inputs=[big_customer_consume_spout['big_customers_stream_spout']],par=3)
    filter_big_customer_log_stream_bolt = FilterLogBigCustomerBolt.spec(inputs=[big_customer_consume_spout],par=6)

    '''big customers calculate consume'''
    vivo_consume_bolt = VIVOTotalBolt.spec(inputs=[filter_big_customer_log_stream_bolt['filter_big_customer_log_stream']], par=2)
    oppo_consume_bolt = OPPOTotalBolt.spec(inputs=[filter_big_customer_log_stream_bolt['filter_big_customer_log_stream']], par=2)
    mi_consume_bolt = MITotalBolt.spec(inputs=[filter_big_customer_log_stream_bolt['filter_big_customer_log_stream']], par=2)



    '''taobao calculate consume'''
    taobao_consume_bolt = TAOBAOTotalBolt.spec(inputs=[filter_big_customer_log_stream_bolt['filter_big_customer_log_stream']], par=6)

