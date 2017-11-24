## metric 参数说明
#### json串
我们使用computeDimension中的配置

```
{
  "alarmConcerner": {
    "tellers": [
      "1360101001",
      "1360101002"
    ],
    "mailTeller": [
      "yourname@company.com"
    ]
  },
  "malarmConditions": [
    {
      "type": 9,
      "period": 300,
      "slideType": 1,
      "threshold": "40"
    },
    {
      "type": 2,
      "period": 1,
      "slideType": 1,
      "threshold": "300"
    }
  ],
  "computeDimension": {
    "computeField": "PutCount",
    "computeType": 2,
    "boltName": "MesaBoltName"
  },
  "mAlarmId": "di_mesa_name_ComparedPutCount",
  "alarmMetric": "di_mesa_bhv_online",
  "alarmType": 2,
  "alarmFrequency": 300,
  "alarmCount": 0,
  "alarmTriggerCount": "1",
  "isAlarm": 1,
  "connectionType": "and",
  "template": "[告警]实时流任务[${name}] 行为数据波动异常 ,告警时间 ${time} , 告警内容:{${record}}"
}
```
#### computeDimension 字段说明(监控)
- boltName 对应jstorm流数据BoltName
- computeField 需要监控的字段,opentsdb中,作为tagK
- computeType,取值如下,未标注的未实现
    - `1 PV`
    - `2 SUM`
    - `3 AVG`
    - `4 MAX`
    - `5 MIN`
    - `6 UV`
    - 7 MAP_PV
    - 8 MAP_SUM
    - 9 MAP_AVG
    - 10 MAP_MAX
    - 11 MAP_MIN
    - 12 TOPN
    - 13 Mutiple（指标间的计算）

#### malarmConditions字段说明(告警设置)
- computeField 需要监控的字段,opentsdb中,作为tagK
- computeType,取值如下,未标注的未实现
    - 1、大于
    - 2、大于等于
    - 3、小于
    - 4、小于等于
    - 5、和上一次差值大于
    - 6、和上一次差值大于等于
    - 7、和上一次差值小于
    - 8、和上一次差值小于等于
    - 9、同比
    - 10、环比
    - 11、最大值
    - 12、最小值
    - 13、连续同步

#### alarmConcerner 字段说明(告警通知人设置)
- tellers 电话联系人
- mailTeller 邮件联系人
- weixinTeller 微信联系人

#### template 告警模板
```
  "template":"[告警]实时流任务[${name}] ,退款数据延迟 ,告警时间 ${time} ,延迟时间 ${value} 分钟 ,告警内容 {${record}}"
```

- ${name} 告警ID
- ${time} 为粗略的时间.为告警开始时间和结束时间的均值.当告警间隔时间是与统计间隔时间不一致时,会带来一定误差.这里取大概的时间.比如间隔为5分钟,短信中收到的时间为10:58分,实际告警统计的时间段为[10:56-11:00]
- ${value} 监控到的值,与阀值相对比
- ${record} 告警实际内容,详细些,如
  `[告警]实时流任务[di_mesa_order_online_CompardPutCount] 订单数据波动异常 ,告警时间 21:00 ,告警内容:{>=300.0,>=300.0,>=300.0,>=300.0,>=300.0,>=300.0； 同比：5m -67.2%，5m +52.47%，5m +132.19%，5m -40.01%，5m
 +49.1%，5m +119.93%}`
