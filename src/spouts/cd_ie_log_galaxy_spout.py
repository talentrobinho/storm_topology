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


class CdIeLogGalaxySpout(Spout):
    outputs = ['cd_ie_log', 'server_ip']
    #outputs = [Stream(fields=['cd_ie_log', 'server_ip'], name='big_customers_stream_spout')]

    def initialize(self, stormconf, context):

        #broker = ParseConf.kafka['broker']
        #tmp = int(time.time())
        #group = 'CalculatedConsumption_{}'.format(tmp)
        #group = 'CalculatedConsumption_test'
        #conf = {'bootstrap.servers': broker, 'group.id': group, 'session.timeout.ms': 6000,'default.topic.config': {'auto.offset.reset': 'latest'}}
        #self.consumer = Consumer(conf)
        self.redis = None
        self.message = None
        self.messageid = None
        self.server_ip = None
        self.line_value = None
        self.line_offset = None
        self.kafka_group = 'galaxy'
        self.kafka_subscribe = 'cd_ie_log'

        self.conndb = ConnDB()
        self.kafka = self.conndb.conn_kafka(self.kafka_group)

    def activate(self):
        self.kafka.subscribe([self.kafka_subscribe])

    def next_tuple(self):
        #line = self.consumer.poll()
        #self.emit([line.value().decode('gbk', 'ignore').encode('utf8')])
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
        #self.logger.info(self.line.value().decode('gbk', 'ignore').encode('utf8', 'ignore'))
        self.line_value = self.line.value().decode('gbk', 'ignore')
        self.line_offset = self.line.offset()
        #self.logger.info("%s\t%s"%(self.line_offset, self.line_value))
        if not self.line_value:

            self.server_ip = '0.0.0.0'
            self.message = 'NOMESSAGE'
            #self.logger.info("<<< %s\t%s >>>"%(self.message,self.server_ip))
            #self.emit([self.message, self.server_ip], stream='big_customers_stream_spout')
            self.emit([self.message, self.server_ip])
        else:   
            self.server_ip = self.line.key().split('_')[0]
            #for line in self.consumer.poll().value().split('\n'):
            #for line in self.line.value().split('\n'):
            for line in self.line_value.split('\n'):
                #self.message = line.decode('gbk', 'ignore').encode('utf8', 'ignore')
                #self.messageid = hashlib.md5(self.message).hexdigest()
                #self.emit([self.message], tup_id=self.messageid)
                #self.emit([self.message, self.server_ip])
                #self.logger.info("%s\t%s"%(self.line_offset, line))
                #self.emit([line, self.server_ip], stream='big_customers_stream_spout')
                self.emit([line, self.server_ip])


    def deactivate(self):
        self.kafka.close()

    #def ack(self, tup_id):
    #    self.redis.publish('watch_log', '{}:\t{}'.format('succ',tup_id))

    #def fail(self, tup_id):
    #    self.redis.publish('watch_log', '{}:\t{}'.format('fail',tup_id))
