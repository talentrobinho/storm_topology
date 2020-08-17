"""
big customers topology
"""

from streamparse import Grouping, Topology

from spouts.cd_ie_log_galaxy_spout import CdIeLogGalaxySpout
from bolts.filter_log import FilterLogBolt
from bolts.calculate_galaxy_consume import *



class CalculateGalaxyConsume(Topology):
    galaxy_consume_spout = CdIeLogGalaxySpout.spec(par=3)
    filter_galaxy_stream_bolt = FilterLogBolt.spec(inputs=[galaxy_consume_spout],par=3)

    '''galaxy customers calculate consume'''
    galaxy_ec_consume = GalaxyECBolt.spec(inputs=[filter_galaxy_stream_bolt['filter_log_stream']], par=2)
    wg_finance_consume = WGFinanceBolt.spec(inputs=[filter_galaxy_stream_bolt['filter_log_stream']], par=2)
    wg_other_consume = WGOtherBolt.spec(inputs=[filter_galaxy_stream_bolt['filter_log_stream']], par=2)
    wg_moon_ec_consume = MoonECBolt.spec(inputs=[filter_galaxy_stream_bolt['filter_log_stream']], par=2)


