from itertools import cycle
import sys
import hashlib
import time


from config.parseconf import ParseConf
from streamparse import Tuple, Spout, ReliableSpout
from confluent_kafka import Consumer, KafkaError, TopicPartition
from conndb.conndatabase import ConnDB

class BaseServerLogSpout(Spout):

    outputs = ['ws_bd_ie', 'server_ip']

    def initialize(self, stormconf, context):

        self.message = None
        self.server_ip = None
        self.kafka_group = 'wireless_search'
        self.kafka_subscribe = 'ws_bd_ie'

        self.conndb = ConnDB()
        self.kafka = self.conndb.conn_kafka(self.kafka_group)


    def activate(self):
        self.kafka.subscribe([self.kafka_subscribe])

    def next_tuple(self):
        self.line = self.kafka.poll()
        self.line_value = self.line.value().decode('gbk', 'ignore')
        self.line_offset = self.line.offset()

        #self.logger.info(">>>>> offset: %s"%self.line.value())
        if not self.line.value():
            self.server_ip = '0.0.0.0'
            self.message = 'NOMESSAGE'
            self.emit([self.message, self.server_ip])
        else:
            self.server_ip = self.line.key().split('_')[0]
            for line in self.line_value.split('\n'):
                #self.logger.info(">>>>> : %s"%line)
                #self.message = line.decode('gbk', 'ignore').encode('utf8', 'ignore')
                self.message = line.encode('utf8', 'ignore')
                self.emit([self.message, self.server_ip])

    def deactivate(self):
        self.kafka.close()




class WSIeLogSpout(BaseServerLogSpout):
    
    outputs = ['ws_bd_ie', 'server_ip']

    def initialize(self, stormconf, context):
        self.message = None
        self.server_ip = None
        ### clickhouse useing
        ### self.kafka_group = 'wireless_search2'
        ### influxdb useing
        self.kafka_group = 'ws2'
        self.kafka_subscribe = 'ws_bd_ie'

        self.conndb = ConnDB()
        self.kafka = self.conndb.conn_kafka(self.kafka_group)


    def activate(self):
        self.kafka.subscribe([self.kafka_subscribe])


