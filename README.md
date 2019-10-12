# influxSink

flume 1.9 通用的业务日志导送到influxdb里面用于业务展示

配置说明：
日志格式设置：这里采用正则对日志格式进行解析，agent.sinks.k1.regex = (\\d{2}-.*-\\d{3}.*\\d{2}:\\d{2}:\\d{2}).* (PHP.*: ) (.*)
该配置主要是你自己的日志想要挑选出来的字段，要与下面的对应上，比方第一个小括号里面我挑出来的是日期，我字段名定义为data

Influxdb配置说明：

agent.sinks.k1.host = 10.100.135.78

上面是influxdb服务器地址

agent.sinks.k1.port = 8086

上面是influxdb服务器端口

agent.sinks.k1.username = root

上面是influxdb服务器账号

agent.sinks.k1.password = root

上面是influxdb服务器密码

agent.sinks.k1.database = flume

上面是influxdb服务器数据库名

agent.sinks.k1.policy = flume

上面是influxdb服务器该数据库下你打算使用的保留策略，不填这项默认是default

agent.sinks.k1.measurement = flume

上面是influxdb服务器表明

agent.sinks.k1.include = ""

上面是日志必须包含哪些字段才能进入过滤多个字段用英文逗号分隔,

agent.sinks.k1.exclude = ""

上面是日志必须包含哪些字段不能进入过滤多个字段用英文逗号分隔,


下面是具体配置

agent.sources = r1

agent.channels = c1

agent.sinks = k1

# For each one of the sources, the type is defined

agent.sources.r1.type = exec

agent.sources.r1.command = tail -f /data/log/php_error.log

agent.sources.r1.interceptors = i1 i2

agent.sources.r1.interceptors.i1.type = host

agent.sources.r1.interceptors.i1.hostHeader  = hostname

agent.sources.r1.interceptors.i1.useIP  = true

agent.sources.r1.interceptors.i2.type = static

agent.sources.r1.interceptors.i2.key = type

agent.sources.r1.interceptors.i2.value = php

# The channel can be defined as follows.

agent.channels.c1.type = memory

agent.channels.c1.capacity = 4096

agent.channels.c1.transactionCapacity = 2048

# Each sink's type must be defined

agent.sinks.k1.type = com.muke.influxdb.InfluxSink

agent.sinks.k1.host = x.x.x.x

agent.sinks.k1.port = 8086

agent.sinks.k1.username = root

agent.sinks.k1.password = root

agent.sinks.k1.database = flume

agent.sinks.k1.regex = (\\d{2}-.*-\\d{3}.*\\d{2}:\\d{2}:\\d{2}).* (PHP.*: ) (.*)

agent.sinks.k1.measurement = flume

agent.sinks.k1.colume = date,level,content

agent.sinks.k1.policy = flume

agent.sources.r1.channels = c1

agent.sinks.k1.channel = c1
