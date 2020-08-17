#coding=utf-8
from itertools import cycle
import sys
import hashlib
import time


from config.parseconf import ParseConf
from streamparse import Tuple, Spout, ReliableSpout
from streamparse import Stream
from confluent_kafka import Consumer, KafkaError, TopicPartition
from conndb.conndatabase import ConnDB

'''
在有汉字需要进行转码时，出现报错加上此2句就可以解决报错问题
'''
reload(sys)
sys.setdefaultencoding( "utf-8" )


class WsIeLogSpout(Spout):
    #outputs = ['cd_ie_log', 'server_ip']
    outputs = [Stream(fields=['ws_ie_log', 'server_ip'], name='wsielog_spout')]

    def initialize(self, stormconf, context):
        self.redis = None
        self.message = None
        self.messageid = None
        self.server_ip = None
        self.line_value = None
        self.line_offset = None
        self.kafka_group = 'wst1'
        self.kafka_subscribe = 'wsielog'

        self.conndb = ConnDB()
        self.kafka = self.conndb.conn_bak_kafka(self.kafka_group)


    def activate(self):
        self.kafka.subscribe([self.kafka_subscribe])

    def next_tuple(self):
        self.line = self.kafka.poll()

        '''
        self.consumer.poll() Object properties
        ['__class__', '__delattr__', '__doc__', '__format__', '__getattribute__', 
         '__hash__', '__init__', '__len__', '__new__', '__reduce__', '__reduce_ex__', 
         '__repr__', '__setattr__', '__sizeof__', '__str__', '__subclasshook__', 
         'error', 
         'headers', 
         'key', 
         'offset', 
         'partition', 
         'set_headers', 
         'set_key', 
         'set_value', 
         'timestamp', 
         'topic', 
         'value']
        '''
        self.line_value = self.line.value()
        self.line_offset = self.line.offset()
        #if not self.line_value:
        #    self.server_ip = '0.0.0.0'
        #    self.message = 'NOMESSAGE'
        #    self.emit([self.message, self.server_ip], stream='cdielog_reliable_spout')
        if self.line.error():
            self.logger.error("WsIeLogSpout error: {}".format(self.line.error()))
        elif self.line_value: 
            self.line_value = self.line_value.decode('gbk', 'ignore')
            self.server_ip = self.line.key().split('_')[0]
            for line in self.line_value.split('\n'):
                self.emit([line, self.server_ip], stream='wsielog_spout')


    def deactivate(self):
        self.kafka.close()

    #def ack(self, tup_id):
    #    self.redis.publish('watch_log', '{}:\t{}'.format('succ',tup_id))

    #def fail(self, tup_id):
    #    self.redis.publish('watch_log', '{}:\t{}'.format('fail',tup_id))
