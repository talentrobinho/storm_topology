from itertools import cycle
import sys
import hashlib
import time


from config.parseconf import ParseConf
from streamparse import Tuple, Spout, ReliableSpout
from confluent_kafka import Consumer, KafkaError, TopicPartition
from conndb.conndatabase import ConnDB

class BaseServerLogSpout(Spout):

    outputs = ['cd_ie_log', 'server_ip']

    def initialize(self, stormconf, context):

        self.message = None
        self.server_ip = None
        self.kafka_group = 'bill_click'
        self.kafka_subscribe = 'cd_ie_log'

        self.conndb = ConnDB()
        self.kafka = self.conndb.conn_kafka(self.kafka_group)


    def activate(self):
        self.kafka.subscribe([self.kafka_subscribe])

    def next_tuple(self):
        self.line = self.kafka.poll()
        if self.line:
            self.server_ip = self.line.key().split('_')[0]
            for line in self.line.value().split('\n'):
                self.message = line.decode('gbk', 'ignore').encode('utf8', 'ignore')
                self.emit([self.message, self.server_ip])
        else:   
            self.server_ip = '0.0.0.0'
            self.message = 'NOMESSAGE'
            self.emit([self.message, self.server_ip])

    def deactivate(self):
        self.kafka.close()




class CdIeLogSpout(BaseServerLogSpout):

    outputs = ['cd_ie_log', 'server_ip']
    def activate(self):
        self.kafka.subscribe(['cd_ie_log'])


class BillIeLogSpout(BaseServerLogSpout):

    outputs = ['bill_ie_log', 'server_ip']

    def activate(self):
        #super(BillIeLogSpout, self).process('bill_ie_log')
        self.kafka.subscribe(['bill_ie_log'])




###################################################
###class CdIeLogSpout(Spout):
###    outputs = ['cd_ie_log', 'server_ip']
###
###    def initialize(self, stormconf, context):
###
###        self.message = None
###        self.server_ip = None
###
###        self.conndb = ConnDB()
###        self.kafka = self.conndb.conn_kafka()
###
###
###    def activate(self):
###        self.kafka.subscribe(['cd_ie_log'])
###
###    def next_tuple(self):
###        self.line = self.kafka.poll()
###        if not self.line:
###            return 0
###        self.server_ip = self.line.key().split('_')[0]
###        for line in self.line.value().split('\n'):
###            self.message = line.decode('gbk', 'ignore').encode('utf8', 'ignore')
###            self.emit([self.message, self.server_ip])
###
###    def deactivate(self):
###        self.kafka.close()
###
###
###class BillIeLogSpout(Spout):
###    outputs = ['bill_ie_log', 'server_ip']
###
###    def initialize(self, stormconf, context):
###
###        self.message = None
###        self.server_ip = None
###
###        self.conndb = ConnDB()
###        self.kafka = self.conndb.conn_kafka()
###
###
###    def activate(self):
###        self.kafka.subscribe(['bill_ie_log'])
###
###    def next_tuple(self):
###        self.line = self.kafka.poll()
###        if not self.line:
###            return 0
###        self.server_ip = self.line.key().split('_')[0]
###        for line in self.line.value().split('\n'):
###            self.message = line.decode('gbk', 'ignore').encode('utf8', 'ignore')
###            self.emit([self.message, self.server_ip])
###
###    def deactivate(self):
###        self.kafka.close()
###
