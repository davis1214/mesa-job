package com.di.mesa.job.jstorm.spout

import java.util

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Values
import backtype.storm.utils.Utils
import java.util.Map
import java.util.Random


/**
  * Created by Administrator on 17/6/29.
  */
class SampleDataSpout(var data: String) extends BaseRichSpout {
  private var _collector: SpoutOutputCollector = _


  val log =
    """"requestTime:2017-06-28 11:00:00"|"requestTime_ms:2017-06-28 11:00:00 033"|"requesturl:http://gw.api.mesa.com/app/797/wd/pb/push/upload_push_info"|"x-real-ip:175.42.201.220"|"reqMethod:POST"|"scheme:https"|"X-Real-Port:51141"|"remote_port:51141"|"remote_addr:10.2.100.37"|"http_x_forwarded_for:175.42.201.220"|"traceID:62wfpj4gexuox22ca"|"open_id:E407938D-A531-4C51-929C-DD5A60C3CAF5"|"proxysign:52C17B37A3594998"|"notifyStatus:1"|"channel:1000f"|"userScene:3"|"sessionid:ks_1_1498618789286_893935_100"|"userID:1192277762"|"machineName:iPhone8,2"|"wduss:7605292c31baec5c97fcd20799859a795451647914114f6ee18926a733244f12a60b0649f357ba9f94d4962facd47628"|"net_timestamp_type:server"|"idfv:E756CD74-902A-47A2-AECA-1924ECED74BD"|"brand:Apple"|"lat:25.1163465050098"|"device_id:114882375"|"idfa:8E4C598D-ED6A-41A4-AA87-821AC6966509"|"alt:375.4355163574219"|"keyid:1.1.1"|"appstatus:background"|"version:7.9.7"|"signid:4.1"|"wssid:FAST_BC5A"|"access_token:AAAAAcI/in2tH0jtt51/hVgIz/5YKLD3IcO4eU4U0jKltpbVC97kMSQBZEwjglfKScIJIFwneWlKDrkIATlRXEs47s93UB6g1fItP1ei93E+f+6EXuNUpu/Nj7KpzieFZR5LMsQy2lBpMBt2cr1KFBl3Bgg."|"shop_id:1192277762"|"apiv:797"|"guid:1498480929307_3227642"|"loc:117.0213689838664,25.1163465050098,375.4355163574219"|"userStatus:1"|"kid:1.1.1"|"bundleid:com.mesa.mesa"|"mid:iPhone"|"lon:117.0213689838664"|"imsi:"|"mac:02:00:00:00:00:00"|"platform:iphone"|"openudid:355e32fe6fdad166eca912474873104603caf40e"|"network:WIFI"|"wmac:78:eb:14:4a:bc:5a"|"encryType:2"|"os:9.3"|"gzipType:1"|"pushTypes:[{\"pushStatus\":\"1\",\"pushType\":\"2\",\"token\":\"782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097\",\"refreshed\":\"0\"},{\"pushStatus\":\"1\",\"pushType\":\"20\",\"token\":\"\",\"refreshed\":\"0\"}]"|"h:2208"|"token:782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097"|"device_id_v2:6eef4802b89f6df6e29417b3244a1294+89f9991039df6a90cd0972d8071e173e"|"build:20170620200317"|"appid:com.mesa.weishop"|"w:1242"|"netsubtype:"|"imei:"|"net_timestamp:1498618799997"|"wfr:null"|"real_apiv:0"|"rmethod:wd/pb/push/upload_push_info"|"executeTime:84" |"""

  //System.currentTimeMillis) + " " + hash + a + "0a010f50" + hash1 + " 0.1.1 1 1|1|/final.do|payCashier|10.1.15.80|-|0|15|1|200"

  val log1 = "\"requestTime:2017-06-28 11:00:00\"|\"requestTime_ms:2017-06-28 11:00:00 033\"|\"requesturl:http://gw.api.mesa" +
    ".com/app/797/wd/pb/push/upload_push_info\"|\"x-real-ip:175.42.201.220\"|\"reqMethod:POST\"|\"scheme:https\"|\"X-Real-Port:51141\"|\"remote_port:51141\"|\"remote_addr:10.2.100.37\"|\"http_x_forwarded_for:175.42.201.220\"|\"traceID:62wfpj4gexuox22ca\"|\"open_id:E407938D-A531-4C51-929C-DD5A60C3CAF5\"|\"proxysign:52C17B37A3594998\"|\"notifyStatus:1\"|\"channel:1000f\"|\"userScene:3\"|\"sessionid:ks_1_1498618789286_893935_100\"|\"userID:1192277762\"|\"machineName:iPhone8,2\"|\"wduss:7605292c31baec5c97fcd20799859a795451647914114f6ee18926a733244f12a60b0649f357ba9f94d4962facd47628\"|\"net_timestamp_type:server\"|\"idfv:E756CD74-902A-47A2-AECA-1924ECED74BD\"|\"brand:Apple\"|\"lat:25.1163465050098\"|\"device_id:114882375\"|\"idfa:8E4C598D-ED6A-41A4-AA87-821AC6966509\"|\"alt:375.4355163574219\"|\"keyid:1.1.1\"|\"appstatus:background\"|\"version:7.9.7\"|\"signid:4.1\"|\"wssid:FAST_BC5A\"|\"access_token:AAAAAcI/in2tH0jtt51/hVgIz/5YKLD3IcO4eU4U0jKltpbVC97kMSQBZEwjglfKScIJIFwneWlKDrkIATlRXEs47s93UB6g1fItP1ei93E+f+6EXuNUpu/Nj7KpzieFZR5LMsQy2lBpMBt2cr1KFBl3Bgg.\"|\"shop_id:1192277762\"|\"apiv:797\"|\"guid:1498480929307_3227642\"|\"loc:117.0213689838664,25.1163465050098,375.4355163574219\"|\"userStatus:1\"|\"kid:1.1.1\"|\"bundleid:com.mesa.mesa\"|\"mid:iPhone\"|\"lon:117.0213689838664\"|\"imsi:\"|\"mac:02:00:00:00:00:00\"|\"platform:iphone\"|\"openudid:355e32fe6fdad166eca912474873104603caf40e\"|\"network:WIFI\"|\"wmac:78:eb:14:4a:bc:5a\"|\"encryType:2\"|\"os:9.3\"|\"gzipType:1\"|\"pushTypes:[{\"pushStatus\":\"1\",\"pushType\":\"2\",\"token\":\"782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097\",\"refreshed\":\"0\"},{\"pushStatus\":\"1\",\"pushType\":\"20\",\"token\":\"\",\"refreshed\":\"0\"}]\"|\"h:2208\"|\"token:782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097\"|\"device_id_v2:6eef4802b89f6df6e29417b3244a1294+89f9991039df6a90cd0972d8071e173e\"|\"build:20170620200317\"|\"appid:com.mesa.weishop\"|\"w:1242\"|\"netsubtype:\"|\"imei:\"|\"net_timestamp:1498618799997\"|\"wfr:null\"|\"real_apiv:0\"|\"rmethod:wd/pb/push/upload_push_info\"|\"executeTime:84\" |";
  val log2 = "\"requestTime:2017-06-28 11:00:00\"|\"requestTime_ms:2017-06-28 11:00:00 033\"|\"requesturl:https://gwh5.api.mesa.com/wd/order/seller/getCompleteTaskCnt\"|\"x-real-ip:175.42.201.220\"|\"reqMethod:POST\"|\"scheme:https\"|\"X-Real-Port:51141\"|\"remote_port:51141\"|\"remote_addr:10.2.100.37\"|\"http_x_forwarded_for:175.42.201.220\"|\"traceID:62wfpj4gexuox22ca\"|\"open_id:E407938D-A531-4C51-929C-DD5A60C3CAF5\"|\"proxysign:52C17B37A3594998\"|\"notifyStatus:1\"|\"channel:1000f\"|\"userScene:3\"|\"sessionid:ks_1_1498618789286_893935_100\"|\"userID:1192277762\"|\"machineName:iPhone8,2\"|\"wduss:7605292c31baec5c97fcd20799859a795451647914114f6ee18926a733244f12a60b0649f357ba9f94d4962facd47628\"|\"net_timestamp_type:server\"|\"idfv:E756CD74-902A-47A2-AECA-1924ECED74BD\"|\"brand:Apple\"|\"lat:25.1163465050098\"|\"device_id:114882375\"|\"idfa:8E4C598D-ED6A-41A4-AA87-821AC6966509\"|\"alt:375.4355163574219\"|\"keyid:1.1.1\"|\"appstatus:background\"|\"version:7.9.7\"|\"signid:4.1\"|\"wssid:FAST_BC5A\"|\"access_token:AAAAAcI/in2tH0jtt51/hVgIz/5YKLD3IcO4eU4U0jKltpbVC97kMSQBZEwjglfKScIJIFwneWlKDrkIATlRXEs47s93UB6g1fItP1ei93E+f+6EXuNUpu/Nj7KpzieFZR5LMsQy2lBpMBt2cr1KFBl3Bgg.\"|\"shop_id:1192277762\"|\"apiv:797\"|\"guid:1498480929307_3227642\"|\"loc:117.0213689838664,25.1163465050098,375.4355163574219\"|\"userStatus:1\"|\"kid:1.1.1\"|\"bundleid:com.mesa.mesa\"|\"mid:iPhone\"|\"lon:117.0213689838664\"|\"imsi:\"|\"mac:02:00:00:00:00:00\"|\"platform:iphone\"|\"openudid:355e32fe6fdad166eca912474873104603caf40e\"|\"network:WIFI\"|\"wmac:78:eb:14:4a:bc:5a\"|\"encryType:2\"|\"os:9.3\"|\"gzipType:1\"|\"pushTypes:[{\"pushStatus\":\"1\",\"pushType\":\"2\",\"token\":\"782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097\",\"refreshed\":\"0\"},{\"pushStatus\":\"1\",\"pushType\":\"20\",\"token\":\"\",\"refreshed\":\"0\"}]\"|\"h:2208\"|\"token:782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097\"|\"device_id_v2:6eef4802b89f6df6e29417b3244a1294+89f9991039df6a90cd0972d8071e173e\"|\"build:20170620200317\"|\"appid:com.mesa.weishop\"|\"w:1242\"|\"netsubtype:\"|\"imei:\"|\"net_timestamp:1498618799997\"|\"wfr:null\"|\"real_apiv:0\"|\"rmethod:wd/pb/push/upload_push_info\"|\"executeTime:84\" |";
  val log3 = "\"requestTime:2017-06-28 11:00:00\"|\"requestTime_ms:2017-06-28 11:00:00 033\"|\"requesturl:https://gwh5.api.mesa.com/wd/shop/base/getShopFlagInfo\"|\"x-real-ip:175.42.201.220\"|\"reqMethod:POST\"|\"scheme:https\"|\"X-Real-Port:51141\"|\"remote_port:51141\"|\"remote_addr:10.2.100.37\"|\"http_x_forwarded_for:175.42.201.220\"|\"traceID:62wfpj4gexuox22ca\"|\"open_id:E407938D-A531-4C51-929C-DD5A60C3CAF5\"|\"proxysign:52C17B37A3594998\"|\"notifyStatus:1\"|\"channel:1000f\"|\"userScene:3\"|\"sessionid:ks_1_1498618789286_893935_100\"|\"userID:1192277762\"|\"machineName:iPhone8,2\"|\"wduss:7605292c31baec5c97fcd20799859a795451647914114f6ee18926a733244f12a60b0649f357ba9f94d4962facd47628\"|\"net_timestamp_type:server\"|\"idfv:E756CD74-902A-47A2-AECA-1924ECED74BD\"|\"brand:Apple\"|\"lat:25.1163465050098\"|\"device_id:114882375\"|\"idfa:8E4C598D-ED6A-41A4-AA87-821AC6966509\"|\"alt:375.4355163574219\"|\"keyid:1.1.1\"|\"appstatus:background\"|\"version:7.9.7\"|\"signid:4.1\"|\"wssid:FAST_BC5A\"|\"access_token:AAAAAcI/in2tH0jtt51/hVgIz/5YKLD3IcO4eU4U0jKltpbVC97kMSQBZEwjglfKScIJIFwneWlKDrkIATlRXEs47s93UB6g1fItP1ei93E+f+6EXuNUpu/Nj7KpzieFZR5LMsQy2lBpMBt2cr1KFBl3Bgg.\"|\"shop_id:1192277762\"|\"apiv:797\"|\"guid:1498480929307_3227642\"|\"loc:117.0213689838664,25.1163465050098,375.4355163574219\"|\"userStatus:1\"|\"kid:1.1.1\"|\"bundleid:com.mesa.mesa\"|\"mid:iPhone\"|\"lon:117.0213689838664\"|\"imsi:\"|\"mac:02:00:00:00:00:00\"|\"platform:iphone\"|\"openudid:355e32fe6fdad166eca912474873104603caf40e\"|\"network:WIFI\"|\"wmac:78:eb:14:4a:bc:5a\"|\"encryType:2\"|\"os:9.3\"|\"gzipType:1\"|\"pushTypes:[{\"pushStatus\":\"1\",\"pushType\":\"2\",\"token\":\"782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097\",\"refreshed\":\"0\"},{\"pushStatus\":\"1\",\"pushType\":\"20\",\"token\":\"\",\"refreshed\":\"0\"}]\"|\"h:2208\"|\"token:782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097\"|\"device_id_v2:6eef4802b89f6df6e29417b3244a1294+89f9991039df6a90cd0972d8071e173e\"|\"build:20170620200317\"|\"appid:com.mesa.weishop\"|\"w:1242\"|\"netsubtype:\"|\"imei:\"|\"net_timestamp:1498618799997\"|\"wfr:null\"|\"real_apiv:0\"|\"rmethod:wd/pb/push/upload_push_info\"|\"executeTime:84\" |";
  val log4 = "\"requestTime:2017-06-28 11:00:00\"|\"requestTime_ms:2017-06-28 11:00:00 033\"|\"requesturl:https://gwh5.api.mesa.com/wd/shop/base/getShopFlagInfo\"|\"x-real-ip:175.42.201.220\"|\"reqMethod:POST\"|\"scheme:https\"|\"X-Real-Port:51141\"|\"remote_port:51141\"|\"remote_addr:10.2.100.37\"|\"http_x_forwarded_for:175.42.201.220\"|\"traceID:62wfpj4gexuox22ca\"|\"open_id:E407938D-A531-4C51-929C-DD5A60C3CAF5\"|\"proxysign:52C17B37A3594998\"|\"notifyStatus:1\"|\"channel:1000f\"|\"userScene:3\"|\"sessionid:ks_1_1498618789286_893935_100\"|\"userID:1192277762\"|\"machineName:iPhone8,2\"|\"wduss:7605292c31baec5c97fcd20799859a795451647914114f6ee18926a733244f12a60b0649f357ba9f94d4962facd47628\"|\"net_timestamp_type:server\"|\"idfv:E756CD74-902A-47A2-AECA-1924ECED74BD\"|\"brand:Apple\"|\"lat:25.1163465050098\"|\"device_id:114882375\"|\"idfa:8E4C598D-ED6A-41A4-AA87-821AC6966509\"|\"alt:375.4355163574219\"|\"keyid:1.1.1\"|\"appstatus:background\"|\"version:7.9.7\"|\"signid:4.1\"|\"wssid:FAST_BC5A\"|\"access_token:AAAAAcI/in2tH0jtt51/hVgIz/5YKLD3IcO4eU4U0jKltpbVC97kMSQBZEwjglfKScIJIFwneWlKDrkIATlRXEs47s93UB6g1fItP1ei93E+f+6EXuNUpu/Nj7KpzieFZR5LMsQy2lBpMBt2cr1KFBl3Bgg.\"|\"shop_id:1192277762\"|\"apiv:797\"|\"guid:1498480929307_3227642\"|\"loc:117.0213689838664,25.1163465050098,375.4355163574219\"|\"userStatus:1\"|\"kid:1.1.1\"|\"bundleid:com.mesa.mesa\"|\"mid:iPhone\"|\"lon:117.0213689838664\"|\"imsi:\"|\"mac:02:00:00:00:00:00\"|\"platform:iphone\"|\"openudid:355e32fe6fdad166eca912474873104603caf40e\"|\"network:WIFI\"|\"wmac:78:eb:14:4a:bc:5a\"|\"encryType:2\"|\"os:9.3\"|\"gzipType:1\"|\"pushTypes:[{\"pushStatus\":\"1\",\"pushType\":\"2\",\"token\":\"782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097\",\"refreshed\":\"0\"},{\"pushStatus\":\"1\",\"pushType\":\"20\",\"token\":\"\",\"refreshed\":\"0\"}]\"|\"h:2208\"|\"token:782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097\"|\"device_id_v2:6eef4802b89f6df6e29417b3244a1294+89f9991039df6a90cd0972d8071e173e\"|\"build:20170620200317\"|\"appid:com.mesa.weishop\"|\"w:1242\"|\"netsubtype:\"|\"imei:\"|\"net_timestamp:1498618799997\"|\"wfr:null\"|\"real_apiv:0\"|\"rmethod:wd/pb/push/upload_push_info\"|\"executeTime:84\" |";
  val log5 = "\"requestTime:2017-06-28 11:00:00\"|\"requestTime_ms:2017-06-28 11:00:00 033\"|\"requesturl:https://gwh5.api.mesa.com/wd/shop/base/getShopFlagInfo\"|\"x-real-ip:175.42.201.220\"|\"reqMethod:POST\"|\"scheme:https\"|\"X-Real-Port:51141\"|\"remote_port:51141\"|\"remote_addr:10.2.100.37\"|\"http_x_forwarded_for:175.42.201.220\"|\"traceID:62wfpj4gexuox22ca\"|\"open_id:E407938D-A531-4C51-929C-DD5A60C3CAF5\"|\"proxysign:52C17B37A3594998\"|\"notifyStatus:1\"|\"channel:1000f\"|\"userScene:3\"|\"sessionid:ks_1_1498618789286_893935_100\"|\"userID:1192277762\"|\"machineName:iPhone8,2\"|\"wduss:7605292c31baec5c97fcd20799859a795451647914114f6ee18926a733244f12a60b0649f357ba9f94d4962facd47628\"|\"net_timestamp_type:server\"|\"idfv:E756CD74-902A-47A2-AECA-1924ECED74BD\"|\"brand:Apple\"|\"lat:25.1163465050098\"|\"device_id:114882375\"|\"idfa:8E4C598D-ED6A-41A4-AA87-821AC6966509\"|\"alt:375.4355163574219\"|\"keyid:1.1.1\"|\"appstatus:background\"|\"version:7.9.7\"|\"signid:4.1\"|\"wssid:FAST_BC5A\"|\"access_token:AAAAAcI/in2tH0jtt51/hVgIz/5YKLD3IcO4eU4U0jKltpbVC97kMSQBZEwjglfKScIJIFwneWlKDrkIATlRXEs47s93UB6g1fItP1ei93E+f+6EXuNUpu/Nj7KpzieFZR5LMsQy2lBpMBt2cr1KFBl3Bgg.\"|\"shop_id:1192277762\"|\"apiv:797\"|\"guid:1498480929307_3227642\"|\"loc:117.0213689838664,25.1163465050098,375.4355163574219\"|\"userStatus:1\"|\"kid:1.1.1\"|\"bundleid:com.mesa.mesa\"|\"mid:iPhone\"|\"lon:117.0213689838664\"|\"imsi:\"|\"mac:02:00:00:00:00:00\"|\"platform:iphone\"|\"openudid:355e32fe6fdad166eca912474873104603caf40e\"|\"network:WIFI\"|\"wmac:78:eb:14:4a:bc:5a\"|\"encryType:2\"|\"os:9.3\"|\"gzipType:1\"|\"pushTypes:[{\"pushStatus\":\"1\",\"pushType\":\"2\",\"token\":\"782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097\",\"refreshed\":\"0\"},{\"pushStatus\":\"1\",\"pushType\":\"20\",\"token\":\"\",\"refreshed\":\"0\"}]\"|\"h:2208\"|\"token:782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097\"|\"device_id_v2:6eef4802b89f6df6e29417b3244a1294+89f9991039df6a90cd0972d8071e173e\"|\"build:20170620200317\"|\"appid:com.mesa.weishop\"|\"w:1242\"|\"netsubtype:\"|\"imei:\"|\"net_timestamp:1498618799997\"|\"wfr:null\"|\"real_apiv:0\"|\"rmethod:wd/pb/push/upload_push_info\"|\"executeTime:84\" |";
  val log6 = "\"requestTime:2017-06-28 11:00:00\"|\"requestTime_ms:2017-06-28 11:00:00 033\"|\"requesturl:https://gwh5.api.mesa.com/wd/shop/base/getShopFlagInfo\"|\"x-real-ip:175.42.201.220\"|\"reqMethod:POST\"|\"scheme:https\"|\"X-Real-Port:51141\"|\"remote_port:51141\"|\"remote_addr:10.2.100.37\"|\"http_x_forwarded_for:175.42.201.220\"|\"traceID:62wfpj4gexuox22ca\"|\"open_id:E407938D-A531-4C51-929C-DD5A60C3CAF5\"|\"proxysign:52C17B37A3594998\"|\"notifyStatus:1\"|\"channel:1000f\"|\"userScene:3\"|\"sessionid:ks_1_1498618789286_893935_100\"|\"userID:1192277762\"|\"machineName:iPhone8,2\"|\"wduss:7605292c31baec5c97fcd20799859a795451647914114f6ee18926a733244f12a60b0649f357ba9f94d4962facd47628\"|\"net_timestamp_type:server\"|\"idfv:E756CD74-902A-47A2-AECA-1924ECED74BD\"|\"brand:Apple\"|\"lat:25.1163465050098\"|\"device_id:114882375\"|\"idfa:8E4C598D-ED6A-41A4-AA87-821AC6966509\"|\"alt:375.4355163574219\"|\"keyid:1.1.1\"|\"appstatus:background\"|\"version:7.9.7\"|\"signid:4.1\"|\"wssid:FAST_BC5A\"|\"access_token:AAAAAcI/in2tH0jtt51/hVgIz/5YKLD3IcO4eU4U0jKltpbVC97kMSQBZEwjglfKScIJIFwneWlKDrkIATlRXEs47s93UB6g1fItP1ei93E+f+6EXuNUpu/Nj7KpzieFZR5LMsQy2lBpMBt2cr1KFBl3Bgg.\"|\"shop_id:1192277762\"|\"apiv:797\"|\"guid:1498480929307_3227642\"|\"loc:117.0213689838664,25.1163465050098,375.4355163574219\"|\"userStatus:1\"|\"kid:1.1.1\"|\"bundleid:com.mesa.mesa\"|\"mid:iPhone\"|\"lon:117.0213689838664\"|\"imsi:\"|\"mac:02:00:00:00:00:00\"|\"platform:iphone\"|\"openudid:355e32fe6fdad166eca912474873104603caf40e\"|\"network:WIFI\"|\"wmac:78:eb:14:4a:bc:5a\"|\"encryType:2\"|\"os:9.3\"|\"gzipType:1\"|\"pushTypes:[{\"pushStatus\":\"1\",\"pushType\":\"2\",\"token\":\"782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097\",\"refreshed\":\"0\"},{\"pushStatus\":\"1\",\"pushType\":\"20\",\"token\":\"\",\"refreshed\":\"0\"}]\"|\"h:2208\"|\"token:782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097\"|\"device_id_v2:6eef4802b89f6df6e29417b3244a1294+89f9991039df6a90cd0972d8071e173e\"|\"build:20170620200317\"|\"appid:com.mesa.weishop\"|\"w:1242\"|\"netsubtype:\"|\"imei:\"|\"net_timestamp:1498618799997\"|\"wfr:null\"|\"real_apiv:0\"|\"rmethod:wd/pb/push/upload_push_info\"|\"executeTime:84\" |";
  val log7 = "\"requestTime:2017-06-28 11:00:00\"|\"requestTime_ms:2017-06-28 11:00:00 033\"|\"requesturl:https://gwh5.api.mesa.com/wd/shop/base/getShopFlagInfo\"|\"x-real-ip:175.42.201.220\"|\"reqMethod:POST\"|\"scheme:https\"|\"X-Real-Port:51141\"|\"remote_port:51141\"|\"remote_addr:10.2.100.37\"|\"http_x_forwarded_for:175.42.201.220\"|\"traceID:62wfpj4gexuox22ca\"|\"open_id:E407938D-A531-4C51-929C-DD5A60C3CAF5\"|\"proxysign:52C17B37A3594998\"|\"notifyStatus:1\"|\"channel:1000f\"|\"userScene:3\"|\"sessionid:ks_1_1498618789286_893935_100\"|\"userID:1192277762\"|\"machineName:iPhone8,2\"|\"wduss:7605292c31baec5c97fcd20799859a795451647914114f6ee18926a733244f12a60b0649f357ba9f94d4962facd47628\"|\"net_timestamp_type:server\"|\"idfv:E756CD74-902A-47A2-AECA-1924ECED74BD\"|\"brand:Apple\"|\"lat:25.1163465050098\"|\"device_id:114882375\"|\"idfa:8E4C598D-ED6A-41A4-AA87-821AC6966509\"|\"alt:375.4355163574219\"|\"keyid:1.1.1\"|\"appstatus:background\"|\"version:7.9.7\"|\"signid:4.1\"|\"wssid:FAST_BC5A\"|\"access_token:AAAAAcI/in2tH0jtt51/hVgIz/5YKLD3IcO4eU4U0jKltpbVC97kMSQBZEwjglfKScIJIFwneWlKDrkIATlRXEs47s93UB6g1fItP1ei93E+f+6EXuNUpu/Nj7KpzieFZR5LMsQy2lBpMBt2cr1KFBl3Bgg.\"|\"shop_id:1192277762\"|\"apiv:797\"|\"guid:1498480929307_3227642\"|\"loc:117.0213689838664,25.1163465050098,375.4355163574219\"|\"userStatus:1\"|\"kid:1.1.1\"|\"bundleid:com.mesa.mesa\"|\"mid:iPhone\"|\"lon:117.0213689838664\"|\"imsi:\"|\"mac:02:00:00:00:00:00\"|\"platform:iphone\"|\"openudid:355e32fe6fdad166eca912474873104603caf40e\"|\"network:WIFI\"|\"wmac:78:eb:14:4a:bc:5a\"|\"encryType:2\"|\"os:9.3\"|\"gzipType:1\"|\"pushTypes:[{\"pushStatus\":\"1\",\"pushType\":\"2\",\"token\":\"782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097\",\"refreshed\":\"0\"},{\"pushStatus\":\"1\",\"pushType\":\"20\",\"token\":\"\",\"refreshed\":\"0\"}]\"|\"h:2208\"|\"token:782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097\"|\"device_id_v2:6eef4802b89f6df6e29417b3244a1294+89f9991039df6a90cd0972d8071e173e\"|\"build:20170620200317\"|\"appid:com.mesa.weishop\"|\"w:1242\"|\"netsubtype:\"|\"imei:\"|\"net_timestamp:1498618799997\"|\"wfr:null\"|\"real_apiv:0\"|\"rmethod:wd/pb/push/upload_push_info\"|\"executeTime:84\" |";
  val log8 = "\"requestTime:2017-06-28 11:00:00\"|\"requestTime_ms:2017-06-28 11:00:00 033\"|\"requesturl:https://gwh5.api.mesa.com/wd/shop/base/getShopFlagInfo\"|\"x-real-ip:175.42.201.220\"|\"reqMethod:POST\"|\"scheme:https\"|\"X-Real-Port:51141\"|\"remote_port:51141\"|\"remote_addr:10.2.100.37\"|\"http_x_forwarded_for:175.42.201.220\"|\"traceID:62wfpj4gexuox22ca\"|\"open_id:E407938D-A531-4C51-929C-DD5A60C3CAF5\"|\"proxysign:52C17B37A3594998\"|\"notifyStatus:1\"|\"channel:1000f\"|\"userScene:3\"|\"sessionid:ks_1_1498618789286_893935_100\"|\"userID:1192277762\"|\"machineName:iPhone8,2\"|\"wduss:7605292c31baec5c97fcd20799859a795451647914114f6ee18926a733244f12a60b0649f357ba9f94d4962facd47628\"|\"net_timestamp_type:server\"|\"idfv:E756CD74-902A-47A2-AECA-1924ECED74BD\"|\"brand:Apple\"|\"lat:25.1163465050098\"|\"device_id:114882375\"|\"idfa:8E4C598D-ED6A-41A4-AA87-821AC6966509\"|\"alt:375.4355163574219\"|\"keyid:1.1.1\"|\"appstatus:background\"|\"version:7.9.7\"|\"signid:4.1\"|\"wssid:FAST_BC5A\"|\"access_token:AAAAAcI/in2tH0jtt51/hVgIz/5YKLD3IcO4eU4U0jKltpbVC97kMSQBZEwjglfKScIJIFwneWlKDrkIATlRXEs47s93UB6g1fItP1ei93E+f+6EXuNUpu/Nj7KpzieFZR5LMsQy2lBpMBt2cr1KFBl3Bgg.\"|\"shop_id:1192277762\"|\"apiv:797\"|\"guid:1498480929307_3227642\"|\"loc:117.0213689838664,25.1163465050098,375.4355163574219\"|\"userStatus:1\"|\"kid:1.1.1\"|\"bundleid:com.mesa.mesa\"|\"mid:iPhone\"|\"lon:117.0213689838664\"|\"imsi:\"|\"mac:02:00:00:00:00:00\"|\"platform:iphone\"|\"openudid:355e32fe6fdad166eca912474873104603caf40e\"|\"network:WIFI\"|\"wmac:78:eb:14:4a:bc:5a\"|\"encryType:2\"|\"os:9.3\"|\"gzipType:1\"|\"pushTypes:[{\"pushStatus\":\"1\",\"pushType\":\"2\",\"token\":\"782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097\",\"refreshed\":\"0\"},{\"pushStatus\":\"1\",\"pushType\":\"20\",\"token\":\"\",\"refreshed\":\"0\"}]\"|\"h:2208\"|\"token:782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097\"|\"device_id_v2:6eef4802b89f6df6e29417b3244a1294+89f9991039df6a90cd0972d8071e173e\"|\"build:20170620200317\"|\"appid:com.mesa.weishop\"|\"w:1242\"|\"netsubtype:\"|\"imei:\"|\"net_timestamp:1498618799997\"|\"wfr:null\"|\"real_apiv:0\"|\"rmethod:wd/pb/push/upload_push_info\"|\"executeTime:84\" |";
  val log9 = "\"requestTime:2017-06-28 11:00:00\"|\"requestTime_ms:2017-06-28 11:00:00 033\"|\"requesturl:https://gwh5.api.mesa.com/wd/shop/base/getShopFlagInfo\"|\"x-real-ip:175.42.201.220\"|\"reqMethod:POST\"|\"scheme:https\"|\"X-Real-Port:51141\"|\"remote_port:51141\"|\"remote_addr:10.2.100.37\"|\"http_x_forwarded_for:175.42.201.220\"|\"traceID:62wfpj4gexuox22ca\"|\"open_id:E407938D-A531-4C51-929C-DD5A60C3CAF5\"|\"proxysign:52C17B37A3594998\"|\"notifyStatus:1\"|\"channel:1000f\"|\"userScene:3\"|\"sessionid:ks_1_1498618789286_893935_100\"|\"userID:1192277762\"|\"machineName:iPhone8,2\"|\"wduss:7605292c31baec5c97fcd20799859a795451647914114f6ee18926a733244f12a60b0649f357ba9f94d4962facd47628\"|\"net_timestamp_type:server\"|\"idfv:E756CD74-902A-47A2-AECA-1924ECED74BD\"|\"brand:Apple\"|\"lat:25.1163465050098\"|\"device_id:114882375\"|\"idfa:8E4C598D-ED6A-41A4-AA87-821AC6966509\"|\"alt:375.4355163574219\"|\"keyid:1.1.1\"|\"appstatus:background\"|\"version:7.9.7\"|\"signid:4.1\"|\"wssid:FAST_BC5A\"|\"access_token:AAAAAcI/in2tH0jtt51/hVgIz/5YKLD3IcO4eU4U0jKltpbVC97kMSQBZEwjglfKScIJIFwneWlKDrkIATlRXEs47s93UB6g1fItP1ei93E+f+6EXuNUpu/Nj7KpzieFZR5LMsQy2lBpMBt2cr1KFBl3Bgg.\"|\"shop_id:1192277762\"|\"apiv:797\"|\"guid:1498480929307_3227642\"|\"loc:117.0213689838664,25.1163465050098,375.4355163574219\"|\"userStatus:1\"|\"kid:1.1.1\"|\"bundleid:com.mesa.mesa\"|\"mid:iPhone\"|\"lon:117.0213689838664\"|\"imsi:\"|\"mac:02:00:00:00:00:00\"|\"platform:iphone\"|\"openudid:355e32fe6fdad166eca912474873104603caf40e\"|\"network:WIFI\"|\"wmac:78:eb:14:4a:bc:5a\"|\"encryType:2\"|\"os:9.3\"|\"gzipType:1\"|\"pushTypes:[{\"pushStatus\":\"1\",\"pushType\":\"2\",\"token\":\"782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097\",\"refreshed\":\"0\"},{\"pushStatus\":\"1\",\"pushType\":\"20\",\"token\":\"\",\"refreshed\":\"0\"}]\"|\"h:2208\"|\"token:782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097\"|\"device_id_v2:6eef4802b89f6df6e29417b3244a1294+89f9991039df6a90cd0972d8071e173e\"|\"build:20170620200317\"|\"appid:com.mesa.weishop\"|\"w:1242\"|\"netsubtype:\"|\"imei:\"|\"net_timestamp:1498618799997\"|\"wfr:null\"|\"real_apiv:0\"|\"rmethod:wd/pb/push/upload_push_info\"|\"executeTime:84\" |";
  val log10 = "\"requestTime:2017-06-28 11:00:00\"|\"requestTime_ms:2017-06-28 11:00:00 033\"|\"requesturl:https://gwh5.api.mesa.com/wd/shop/base/getShopFlagInfo\"|\"x-real-ip:175.42.201.220\"|\"reqMethod:POST\"|\"scheme:https\"|\"X-Real-Port:51141\"|\"remote_port:51141\"|\"remote_addr:10.2.100.37\"|\"http_x_forwarded_for:175.42.201.220\"|\"traceID:62wfpj4gexuox22ca\"|\"open_id:E407938D-A531-4C51-929C-DD5A60C3CAF5\"|\"proxysign:52C17B37A3594998\"|\"notifyStatus:1\"|\"channel:1000f\"|\"userScene:3\"|\"sessionid:ks_1_1498618789286_893935_100\"|\"userID:1192277762\"|\"machineName:iPhone8,2\"|\"wduss:7605292c31baec5c97fcd20799859a795451647914114f6ee18926a733244f12a60b0649f357ba9f94d4962facd47628\"|\"net_timestamp_type:server\"|\"idfv:E756CD74-902A-47A2-AECA-1924ECED74BD\"|\"brand:Apple\"|\"lat:25.1163465050098\"|\"device_id:114882375\"|\"idfa:8E4C598D-ED6A-41A4-AA87-821AC6966509\"|\"alt:375.4355163574219\"|\"keyid:1.1.1\"|\"appstatus:background\"|\"version:7.9.7\"|\"signid:4.1\"|\"wssid:FAST_BC5A\"|\"access_token:AAAAAcI/in2tH0jtt51/hVgIz/5YKLD3IcO4eU4U0jKltpbVC97kMSQBZEwjglfKScIJIFwneWlKDrkIATlRXEs47s93UB6g1fItP1ei93E+f+6EXuNUpu/Nj7KpzieFZR5LMsQy2lBpMBt2cr1KFBl3Bgg.\"|\"shop_id:1192277762\"|\"apiv:797\"|\"guid:1498480929307_3227642\"|\"loc:117.0213689838664,25.1163465050098,375.4355163574219\"|\"userStatus:1\"|\"kid:1.1.1\"|\"bundleid:com.mesa.mesa\"|\"mid:iPhone\"|\"lon:117.0213689838664\"|\"imsi:\"|\"mac:02:00:00:00:00:00\"|\"platform:iphone\"|\"openudid:355e32fe6fdad166eca912474873104603caf40e\"|\"network:WIFI\"|\"wmac:78:eb:14:4a:bc:5a\"|\"encryType:2\"|\"os:9.3\"|\"gzipType:1\"|\"pushTypes:[{\"pushStatus\":\"1\",\"pushType\":\"2\",\"token\":\"782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097\",\"refreshed\":\"0\"},{\"pushStatus\":\"1\",\"pushType\":\"20\",\"token\":\"\",\"refreshed\":\"0\"}]\"|\"h:2208\"|\"token:782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097\"|\"device_id_v2:6eef4802b89f6df6e29417b3244a1294+89f9991039df6a90cd0972d8071e173e\"|\"build:20170620200317\"|\"appid:com.mesa.weishop\"|\"w:1242\"|\"netsubtype:\"|\"imei:\"|\"net_timestamp:1498618799997\"|\"wfr:null\"|\"real_apiv:0\"|\"rmethod:wd/pb/push/upload_push_info\"|\"executeTime:84\" |";
  val log11 = "\"requestTime:2017-06-28 11:00:00\"|\"requestTime_ms:2017-06-28 11:00:00 033\"|\"requesturl:https://gwh5.api.mesa.com/wd/shop/base/getShopFlagInfo\"|\"x-real-ip:175.42.201.220\"|\"reqMethod:POST\"|\"scheme:https\"|\"X-Real-Port:51141\"|\"remote_port:51141\"|\"remote_addr:10.2.100.37\"|\"http_x_forwarded_for:175.42.201.220\"|\"traceID:62wfpj4gexuox22ca\"|\"open_id:E407938D-A531-4C51-929C-DD5A60C3CAF5\"|\"proxysign:52C17B37A3594998\"|\"notifyStatus:1\"|\"channel:1000f\"|\"userScene:3\"|\"sessionid:ks_1_1498618789286_893935_100\"|\"userID:1192277762\"|\"machineName:iPhone8,2\"|\"wduss:7605292c31baec5c97fcd20799859a795451647914114f6ee18926a733244f12a60b0649f357ba9f94d4962facd47628\"|\"net_timestamp_type:server\"|\"idfv:E756CD74-902A-47A2-AECA-1924ECED74BD\"|\"brand:Apple\"|\"lat:25.1163465050098\"|\"device_id:114882375\"|\"idfa:8E4C598D-ED6A-41A4-AA87-821AC6966509\"|\"alt:375.4355163574219\"|\"keyid:1.1.1\"|\"appstatus:background\"|\"version:7.9.7\"|\"signid:4.1\"|\"wssid:FAST_BC5A\"|\"access_token:AAAAAcI/in2tH0jtt51/hVgIz/5YKLD3IcO4eU4U0jKltpbVC97kMSQBZEwjglfKScIJIFwneWlKDrkIATlRXEs47s93UB6g1fItP1ei93E+f+6EXuNUpu/Nj7KpzieFZR5LMsQy2lBpMBt2cr1KFBl3Bgg.\"|\"shop_id:1192277762\"|\"apiv:797\"|\"guid:1498480929307_3227642\"|\"loc:117.0213689838664,25.1163465050098,375.4355163574219\"|\"userStatus:1\"|\"kid:1.1.1\"|\"bundleid:com.mesa.mesa\"|\"mid:iPhone\"|\"lon:117.0213689838664\"|\"imsi:\"|\"mac:02:00:00:00:00:00\"|\"platform:iphone\"|\"openudid:355e32fe6fdad166eca912474873104603caf40e\"|\"network:WIFI\"|\"wmac:78:eb:14:4a:bc:5a\"|\"encryType:2\"|\"os:9.3\"|\"gzipType:1\"|\"pushTypes:[{\"pushStatus\":\"1\",\"pushType\":\"2\",\"token\":\"782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097\",\"refreshed\":\"0\"},{\"pushStatus\":\"1\",\"pushType\":\"20\",\"token\":\"\",\"refreshed\":\"0\"}]\"|\"h:2208\"|\"token:782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097\"|\"device_id_v2:6eef4802b89f6df6e29417b3244a1294+89f9991039df6a90cd0972d8071e173e\"|\"build:20170620200317\"|\"appid:com.mesa.weishop\"|\"w:1242\"|\"netsubtype:\"|\"imei:\"|\"net_timestamp:1498618799997\"|\"wfr:null\"|\"real_apiv:0\"|\"rmethod:wd/pb/push/upload_push_info\"|\"executeTime:84\" |";
  val log12 = "\"requestTime:2017-06-28 11:00:00\"|\"requestTime_ms:2017-06-28 11:00:00 033\"|\"requesturl:https://gwh5.api.mesa.com/wd/shop/base/getShopFlagInfo\"|\"x-real-ip:175.42.201.220\"|\"reqMethod:POST\"|\"scheme:https\"|\"X-Real-Port:51141\"|\"remote_port:51141\"|\"remote_addr:10.2.100.37\"|\"http_x_forwarded_for:175.42.201.220\"|\"traceID:62wfpj4gexuox22ca\"|\"open_id:E407938D-A531-4C51-929C-DD5A60C3CAF5\"|\"proxysign:52C17B37A3594998\"|\"notifyStatus:1\"|\"channel:1000f\"|\"userScene:3\"|\"sessionid:ks_1_1498618789286_893935_100\"|\"userID:1192277762\"|\"machineName:iPhone8,2\"|\"wduss:7605292c31baec5c97fcd20799859a795451647914114f6ee18926a733244f12a60b0649f357ba9f94d4962facd47628\"|\"net_timestamp_type:server\"|\"idfv:E756CD74-902A-47A2-AECA-1924ECED74BD\"|\"brand:Apple\"|\"lat:25.1163465050098\"|\"device_id:114882375\"|\"idfa:8E4C598D-ED6A-41A4-AA87-821AC6966509\"|\"alt:375.4355163574219\"|\"keyid:1.1.1\"|\"appstatus:background\"|\"version:7.9.7\"|\"signid:4.1\"|\"wssid:FAST_BC5A\"|\"access_token:AAAAAcI/in2tH0jtt51/hVgIz/5YKLD3IcO4eU4U0jKltpbVC97kMSQBZEwjglfKScIJIFwneWlKDrkIATlRXEs47s93UB6g1fItP1ei93E+f+6EXuNUpu/Nj7KpzieFZR5LMsQy2lBpMBt2cr1KFBl3Bgg.\"|\"shop_id:1192277762\"|\"apiv:797\"|\"guid:1498480929307_3227642\"|\"loc:117.0213689838664,25.1163465050098,375.4355163574219\"|\"userStatus:1\"|\"kid:1.1.1\"|\"bundleid:com.mesa.mesa\"|\"mid:iPhone\"|\"lon:117.0213689838664\"|\"imsi:\"|\"mac:02:00:00:00:00:00\"|\"platform:iphone\"|\"openudid:355e32fe6fdad166eca912474873104603caf40e\"|\"network:WIFI\"|\"wmac:78:eb:14:4a:bc:5a\"|\"encryType:2\"|\"os:9.3\"|\"gzipType:1\"|\"pushTypes:[{\"pushStatus\":\"1\",\"pushType\":\"2\",\"token\":\"782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097\",\"refreshed\":\"0\"},{\"pushStatus\":\"1\",\"pushType\":\"20\",\"token\":\"\",\"refreshed\":\"0\"}]\"|\"h:2208\"|\"token:782d337e814ae51dc73be53abcf4f91669fa88b3e3b5d58590993257813b6097\"|\"device_id_v2:6eef4802b89f6df6e29417b3244a1294+89f9991039df6a90cd0972d8071e173e\"|\"build:20170620200317\"|\"appid:com.mesa.weishop\"|\"w:1242\"|\"netsubtype:\"|\"imei:\"|\"net_timestamp:1498618799997\"|\"wfr:null\"|\"real_apiv:0\"|\"rmethod:wd/pb/push/upload_push_info\"|\"executeTime:84\" |";

  var count = 0;
  val MAX = 6;


  override def nextTuple(): Unit = {
    val MAX: Long = 10

    while (count <= MAX) {
      _collector.emit(new Values(log3));
      _collector.emit(new Values(log1));
      _collector.emit(new Values(log2));
      _collector.emit(new Values(log4));
      _collector.emit(new Values(log5));
      _collector.emit(new Values(log6));
      _collector.emit(new Values(log7));
      _collector.emit(new Values(log8));
      _collector.emit(new Values(log9));
      _collector.emit(new Values(log10));
      _collector.emit(new Values(log11));
      _collector.emit(new Values(log12));
      Utils.sleep(1000);

      println(count + "-------->" + log1);
    }


  }

  override def open(conf: util.Map[_, _], context: TopologyContext, collector: SpoutOutputCollector): Unit = {
    _collector = collector
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("row"))
  }
}