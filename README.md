
#### mesa项目介绍
通用化的任务

#### 任务类型
- strom任务
- spark任务
- spark-streaming任务


#### 开发语言
java(1.8) \scala (2.11.8) \python 3.5


#### 技术栈
* jstorm
* opentsdb
* spark
* hbase
* kafka
* zookeeper
* kudu



##### 路线图
* 整理\梳理基础类型任务
* 实现任务流的管理
* 可视化管理所有的任务流
* 集成任务调度\任务监控管理功能


#### jstorm-plugin
* ZookeeerSpout ``实现全局的动态通知功能``
* RabbitSpout ``扩展实现rabbit mq队列``
* KuduBolt ``增加kudu数据的写入``


#### mesa-metric
提供基于opentsdb的监控告警服务.服务于Mesa实时流服务.
使用方法常见模块README

