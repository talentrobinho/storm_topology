import MySQLdb
import sys
import logging
import time
import urllib
import urllib2
import requests
from config.parseconf import ParseConf
from rediscluster import StrictRedisCluster
from confluent_kafka import Consumer, KafkaError, TopicPartition
from influxdb import InfluxDBClient
from clickhouse_driver import Client
import subprocess
#from random import randint
import random

class ConnDB(object):

    def __init__(self):
        self.logger = logging.getLogger(__name__)

        self.mysql_host = ParseConf.mysql['host']
        self.mysql_user = ParseConf.mysql['user']
        self.mysql_passwd = ParseConf.mysql['passwd']
        self.mysql_db = ParseConf.mysql['db']
        self.mysql_port = ParseConf.mysql['port']

        #self.redis_node_list = ParseConf.bak_redis['nodes']
        self.redis_node_list = ParseConf.redis['nodes']
        #self.redis_bak_node_list = ParseConf.bak_redis['nodes']
        self.conn = None
        self.consumer = None

        self.influxdb_host = ParseConf.influxdb['host']
        self.influxdb_user = ParseConf.influxdb['user']
        self.influxdb_passwd = ParseConf.influxdb['passwd']
        self.influxdb_db = ParseConf.influxdb['db']
        self.influxdb_port = ParseConf.influxdb['port']
        self.influxdb_url = ParseConf.influxdb['url']
        self.influxdb_url_port = ParseConf.influxdb['url_port']


        self.clickhouse_host = ParseConf.clickhouse['host']
        self.clickhouse_user = ParseConf.clickhouse['user']
        self.clickhouse_passwd = ParseConf.clickhouse['passwd']
        self.clickhouse_db = ParseConf.clickhouse['db']
    

        self.ck_cluster_host = ParseConf.clickhouse_cluster['host']
        self.ck_cluster_user = ParseConf.clickhouse_cluster['user']
        self.ck_cluster_passwd = ParseConf.clickhouse_cluster['passwd']
        self.ck_cluster_db = ParseConf.clickhouse_cluster['db']
    def conn_mysql(self):
        try:
            #self.conn = MySQLdb.connect(host="10.134.115.196",user="monitor",passwd="123456",db="zabbix",port=3306,charset="utf8")
            self.conn = MySQLdb.connect(host=self.mysql_host,
                                        user=self.mysql_user,
                                        passwd=self.mysql_passwd,
                                        db=self.mysql_db,
                                        port=self.mysql_port,
                                        charset="utf8")
            #self.cur = self.conn.cursor()
            return self.conn.cursor()
    
        except Exception:
            self.logger.error("Error connecting to MySQL database.")
            sys.exit(1)
    
    def conn_redis(self):
        nodes_index = random.randint(0,len(self.redis_node_list)-1)
        self.logger.info("********* connecting redis_node_list %d ************"%(nodes_index,))
        try:
            #return StrictRedisCluster(startup_nodes=self.redis_node_list, socket_keepalive=True)
            return StrictRedisCluster(startup_nodes=[self.redis_node_list[nodes_index]], max_connections=500)
            #return StrictRedisCluster(startup_nodes=self.redis_node_list, max_connections=500)
        except Exception,e:
            self.logger.error("Filter Error connecting to database of redis cluster.")
            sys.exit(1)
        ###try:
        ###    #self.redis = redis.Redis(host='10.134.15.104', db=1)
        ###    return  redis.Redis(host='10.134.15.104', db=1)
        ###except Exception:
        ###    self.logger.error("Filter Error connecting to database.")
        ###    sys.exit(1)

    def conn_bak_redis(self):
        nodes_index = random.randint(0,len(self.redis_bak_node_list)-1)
        self.logger.info("********* connecting redis_node_list %d ************"%(nodes_index,))
        try:
            return StrictRedisCluster(startup_nodes=[self.redis_bak_node_list[nodes_index]], max_connections=500)
        except Exception,e:
            self.logger.error("Filter Error connecting to database of redis cluster.")
            sys.exit(1)


    def conn_kafka(self, group):
        broker = ParseConf.kafka['broker']
        tmp = int(time.time())
        #group = 'serverlog_{}'.format(tmp)
        #group = 'big_customers'
        #conf = {
        #        'bootstrap.servers': broker,
        #        'group.id': group,
        #        'session.timeout.ms': 600000,
        #        'fetch.wait.max.ms':300000,
        #        'socket.timeout.ms':300000,
        #        'socket.max.fails': 1,
        #        'socket.keepalive.enable': true,
        #        'log_level': 0-7,
        #        'debug': 'generic, \
        #                   broker, \
        #                   topic, \
        #                   metadata, \
        #                   feature, \
        #                   queue, \
        #                   msg, \
        #                   protocol, \
        #                   cgrp, \
        #                   security, \
        #                   fetch, \
        #                   interceptor, \
        #                   plugin, \
        #                   'consumer', \
        #                   admin, \
        #                   eos, \
        #                   all',
        #        'default.topic.config': {'auto.offset.reset': 'latest'}
        #        }
        #conf = {
        #        'bootstrap.servers': broker, 
        #        'group.id': group,
        #        'debug':  'all',
        #        'session.timeout.ms': 60000,
        #        'socket.keepalive.enable': 'true',
        #        'socket.max.fails': 3,
        #        'log_level': 1,
        #        'log.connection.close': 'false',
        #        'default.topic.config': {
        #                                    'auto.offset.reset': 'latest'
        #                                }
        #        }
        conf = {
                'bootstrap.servers': broker, 
                'group.id': group,
                'debug': 'consumer,cgrp,topic',
                'log_level': 3,
                'default.topic.config': {
                                            'auto.offset.reset': 'latest'
                                        }
                }
        #self.consumer = Consumer(conf, logger=self.logger)
        self.consumer = Consumer(conf)
        return self.consumer

    def conn_new_kafka(self, group):
        broker = ParseConf.new_kafka['broker']
        tmp = int(time.time())
        conf = {
                'bootstrap.servers': broker, 
                'group.id': group,
                'debug': 'consumer,cgrp,topic',
                'log_level': 3,
                'default.topic.config': {
                                            'auto.offset.reset': 'latest'
                                        }
                }
        #self.consumer = Consumer(conf, logger=self.logger)
        self.consumer = Consumer(conf)
        return self.consumer

    def conn_js_kafka(self, group):
        broker = ParseConf.js_kafka['broker']
        tmp = int(time.time())
        conf = {
                'bootstrap.servers': broker, 
                'group.id': group,
                'debug': 'consumer,cgrp,topic',
                'log_level': 3,
                'default.topic.config': {
                                            'auto.offset.reset': 'latest'
                                        }
                }
        #self.consumer = Consumer(conf, logger=self.logger)
        self.consumer = Consumer(conf)
        return self.consumer

    def conn_clickhouse(self):
        try:
            client = Client(self.clickhouse_host, 
                            user=self.clickhouse_user, 
                            password=self.clickhouse_passwd, 
                            database=self.clickhouse_db)
            return client
        except Exception as err:
            self.logger.error("Filter Error connecting to database of clickhouse.")
            sys.exit(1)


    def conn_clickhouse_cluster(self):
        err_num = 0
        #for ck_cluster_host in self.ck_cluster_host:
        ck_cluster_host = random.choice(self.ck_cluster_host)
        try:
            client = Client(ck_cluster_host, 
                            user=self.ck_cluster_user, 
                            password=self.ck_cluster_passwd, 
                            database=self.ck_cluster_db)
            return client
        except Exception as err:
            self.logger.error("Filter Error connecting to database of clickhouse cluster[%s]."%(ck_cluster_host,))
            err_num+=1
        if err_num == len(self.ck_cluster_host):
            sys.exit(1)


    def conn_influxdb(self):
        try:
            client = InfluxDBClient(host=self.influxdb_host,
                                    port=self.influxdb_port,
                                    username=self.influxdb_user,
                                    password=self.influxdb_passwd,
                                    database=self.influxdb_db,
                                    pool_size=1000,
                                    timeout=10)
            return client
        except Exception,err:
            self.logger.error("Filter Error connecting to database of influxdb cluster.")
            sys.exit(1)


    def write_influxdb(self, data, db=None):
        self.influxdb_full_url = u"http://%s:%s/write?db=%s"%(self.influxdb_url, self.influxdb_url_port, self.influxdb_db)
        '''
        #if db is None:
        #    #self.influxdb_full_url = "http://{}:{}/write?db={}".format(self.influxdb_url, self.influxdb_url_port, self.influxdb_db)
        #    self.influxdb_full_url = "http://%s:%s/write?db=%s"%(self.influxdb_url, self.influxdb_url_port, self.influxdb_db)
        #else:
        #    self.influxdb_full_url = "http://{}:{}/write?db={}".format(self.influxdb_url, self.influxdb_url_port, db)
        

        header_dict = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
                       'Content-Type': 'application/json'}
        #req = urllib2.Request(url=self.influxdb_full_url, data=urllib.urlencode("write?db=%s --data-binary '%s'"%(self.influxdb_db,data)))
        req = requests.post(self.influxdb_full_url, data=data)
        self.logger.info("%s %s"%(self.influxdb_full_url, data))
        self.logger.info(req.status_code)
        self.logger.info(req.headers)
        #try:
        #    res = urllib2.urlopen(req)
        #    #res = res.read()
        #    #print(res)
        #    res.close()
        #except Exception, err:
        #    self.logger.error('write data to influxdb error [%s]'%err)

        cmd="curl -i -XPOST 'http://10.139.17.51:8086/write?db=realtime' --data-binary '%s'"%(a,)
        '''
        cmd="curl -i -XPOST '%s' --data-binary '%s'"%(self.influxdb_full_url, data)
        p = subprocess.Popen('%s'%(cmd), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        



    def conn_bak_kafka(self, group):
        broker = ParseConf.bak_kafka['broker']
        tmp = int(time.time())

        conf = {
                'bootstrap.servers': broker, 
                'group.id': group,
                'debug': 'consumer,cgrp,topic',
                'log_level': 3,
                'default.topic.config': {
                                            'auto.offset.reset': 'latest'
                                        }
                }
        #self.consumer = Consumer(conf, logger=self.logger)
        self.consumer = Consumer(conf)
        return self.consumer