class ParseConf(object):

    kafka = {}
    bak_kafka = {}
    new_kafka = {}
    js_kafka = {}
    mysql = {}
    influxdb = {}
    redis = {}
    bak_redis = {}
    old_redis = {}
    clickhouse = {}
    clickhouse_cluster = {}

    '''============================ Kafka Configure ============================'''
    #kafka['broker'] = 'kafka02.storm.adt.sogou:9092,kafka03.storm.adt.sogou:9092,kafka04.storm.adt.sogou:9092,kafka05.storm.adt.sogou:9092,kafka06.storm.adt.sogou:9092,kafka07.storm.adt.sogou:9092,kafka01.storm.adt.sogou:9092'
    kafka['broker'] = '10.134.50.125:9092,10.134.50.126:9092,10.134.58.81:9092,10.134.58.82:9092,10.134.24.74:9092,10.134.41.67:9092,10.160.13.77:9092'
    #bak_kafka['broker'] = '10.134.36.218:9092,10.134.43.163:9092,10.134.27.129:9092,10.134.55.208:9092,10.134.54.226:9092,10.134.55.148:9092,10.134.62.205:9092'
    bak_kafka['broker'] = '10.162.133.81:9092,10.162.133.82:9092,10.162.133.83:9092,10.162.133.84:9092,10.162.133.85:9092,10.162.133.86:9092,10.162.133.87:9092,10.162.133.88:9092,10.162.133.89:9092,10.162.133.90:9092,10.162.133.91:9092,10.162.133.92:9092,10.162.133.93:9092,10.162.133.94:9092'
    new_kafka['broker'] = '10.139.36.216:9092,10.139.38.168:9092,10.139.52.209:9092,10.139.24.204:9092,10.139.28.162:9092,10.139.35.178:9092,10.139.33.171:9092,10.139.49.213:9092,10.139.28.179:9092,10.139.31.221:9092,10.139.34.128:9092,10.139.26.228:9092,10.139.17.163:9092,10.139.28.220:9092'
    js_kafka['broker'] = '10.140.48.35:9092,10.140.64.36:9092,10.140.48.38:9092,10.140.56.36:9092,10.140.56.34:9092,10.140.64.34:9092,10.140.48.36:9092'


    '''============================ MySQL Configure ============================'''
    mysql['host'] = 'mysql.storm.adt.sogou'
    mysql['user'] = 'monitor'
    mysql['passwd'] = '123456'
    mysql['port'] = 3306
    mysql['db'] = 'realtime'



    '''============================ Redis Configure ============================'''
    '''redis test
        #10.134.73.67
        #10.134.74.123
        10.134.82.26
        10.134.90.69
        10.134.90.71
    '''

    redis['nodes'] = [{'host': '10.162.129.38', 'port': '7001'},
                      {'host': '10.162.129.38', 'port': '7002'},
                      {'host': '10.162.133.41', 'port': '7001'},
                      {'host': '10.162.133.41', 'port': '7002'},
                      {'host': '10.162.133.42', 'port': '7001'},
                      {'host': '10.162.133.42', 'port': '7002'}]


    '''============================ ClickHouse Configure ============================'''
    clickhouse['host'] = '10.134.49.49'
    clickhouse['user'] = 'storm'
    clickhouse['passwd'] = ''
    clickhouse['port'] = ''
    clickhouse['db'] = 'realtime'


    '''============================ ClickHouse Cluster Configure ============================'''
    #clickhouse_cluster['host'] = ['10.134.113.118', '10.134.92.25', '10.134.76.125', '10.139.36.103']
    clickhouse_cluster['host'] = ['10.139.18.31', '10.139.36.107', '10.139.36.122', '10.139.36.67']
    clickhouse_cluster['user'] = 'storm'
    clickhouse_cluster['passwd'] = ''
    clickhouse_cluster['port'] = ''
    clickhouse_cluster['db'] = 'realtime'

    '''============================ Influxdb Configure ============================'''
    '''
    influxdb['host'] = 'mysql.storm.adt.sogou'
    influxdb['user'] = 'monitor'
    influxdb['passwd'] = 'monitor123'
    influxdb['port'] = 8086
    influxdb['db'] = 'realtime'
    influxdb['url'] = 'mysql.storm.adt.sogou'
    influxdb['url_port'] = 8086
    '''
    influxdb['host'] = '10.139.17.51'
    influxdb['user'] = 'monitor'
    influxdb['passwd'] = 'monitor123'
    influxdb['port'] = 8086
    influxdb['db'] = 'realtime'
    influxdb['url'] = '10.139.17.51'
    influxdb['url_port'] = 8086
