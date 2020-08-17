from itertools import cycle
import sys
import hashlib
import time


from config.parseconf import ParseConf
from streamparse import Tuple, Spout, ReliableSpout, Stream
from confluent_kafka import Consumer, KafkaError, TopicPartition
from conndb.conndatabase import ConnDB

class BaseServerLogSpout(ReliableSpout):

    #outputs = ['ws_bd_ie', 'server_ip']
    outputs = [Stream(fields=['ws_bd_ie', 'server_ip'], name='wsielog_reliable_spout')]

    def initialize(self, stormconf, context):

        self.message = None
        self.server_ip = None
        self.kafka_group = None
        self.kafka_subscribe = None

        self.conndb = ConnDB()
        self.kafka = self.conndb.conn_kafka(self.kafka_group)


    def activate(self):
        self.kafka.subscribe([self.kafka_subscribe])

    def next_tuple(self):
        self.line = self.kafka.poll()
        self.line_value = self.line.value()
        self.line_offset = self.line.offset()




        if self.line_value: 
            self.line_value = self.line_value.decode('gbk', 'ignore')
            self.server_ip = self.line.key().split('_')[0]
            self.line_salt = 0
            for line in self.line_value.split('\n'):
                self.line_salt+=1
                self.messageid = '%s_%s_%d'%( 
                                              self.server_ip, 
                                              self.line_offset, 
                                              self.line_salt)
                self.emit([line, self.server_ip], tup_id=str(self.messageid), stream='wsielog_reliable_spout')


    def deactivate(self):
        self.kafka.close()




class WSIeLogReliableSpout(BaseServerLogSpout):
    
    #outputs = ['ws_bd_ie', 'server_ip']

    def initialize(self, stormconf, context):
        self.message = None
        self.server_ip = None
        ### clickhouse useing
        ### self.kafka_group = 'wireless_search2'
        ### influxdb useing
        self.kafka_group = 'wsielog_reliable'
        self.kafka_subscribe = 'ws_bd_ie'

        self.conndb = ConnDB()
        self.kafka = self.conndb.conn_kafka(self.kafka_group)


    def activate(self):
        self.kafka.subscribe([self.kafka_subscribe])


