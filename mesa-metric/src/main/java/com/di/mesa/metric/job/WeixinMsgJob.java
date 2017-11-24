package com.di.mesa.metric.job;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.di.mesa.metric.util.PropertyUtil;
import com.di.mesa.metric.model.AlarmConcerner;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WeixinMsgJob implements MsgJob {

    private final Logger logger = LoggerFactory.getLogger(WeixinMsgJob.class);

    @Override
    public void sendMsg(AlarmConcerner alarmConcerners, String content) throws IOException {
        String weixinUrl = PropertyUtil.getValueByKey("alarm.properties", "alarm.weixin.url");

        Map<String, String> data = new HashMap<String, String>();
        data.put("user", alarmConcerners.getWeixinStrs());
        data.put("text", content);

        sendWeixinMsg(weixinUrl, data);
    }

//    private void sendWeixinPic(String url, Map<String, String> data, long monitorId) throws IOException {
//
//        HttpClient client = HttpClients.createDefault();
//        HttpPost method = new HttpPost(url);
//        method.addHeader("Content-Type", "application/x-www-form-urlencoded");
//        List<NameValuePair> nvps = new ArrayList<NameValuePair>();
//        for (Map.Entry<String, String> ent : data.entrySet()) {
//            nvps.add(new BasicNameValuePair(ent.getKey(), ent.getValue()));
//        }
//        HttpResponse httpResponse = null;
//        try {
//            method.setEntity(new UrlEncodedFormEntity(nvps, "utf-8"));
//            httpResponse = client.execute(method);
//        } catch (Exception e) {
//            logger.error("httpclient execute get failed!url:" + url, e);
//        }
//        int statusCode = httpResponse.getStatusLine().getStatusCode();
//        if (statusCode == HttpStatus.SC_OK) {
//            try {
//                BufferedReader br = new BufferedReader(new InputStreamReader(httpResponse.getEntity().getContent()));
//                String line = null;
//                StringBuilder sb = new StringBuilder();
//                while ((line = br.readLine()) != null) {
//                    sb.append(line).append("/n");
//                }
//                logger.info("weixin callback {}", sb.substring(0, sb.length() - 1));
//            } catch (Exception e) {
//                e.printStackTrace();
//                logger.error("httpclient read data failed!url:" + url, e);
//            }
//        } else {
//            logger.error("httpclient response statuscode is " + statusCode + "!url:" + url);
//        }
//    }


    private void sendWeixinMsg(String url, Map<String, String> data) throws IOException {

        HttpClient client = HttpClients.createDefault();
        HttpPost method = new HttpPost(url);
        method.addHeader("Content-Type", "application/x-www-form-urlencoded");
        List<NameValuePair> nvps = new ArrayList<NameValuePair>();
        for (Map.Entry<String, String> ent : data.entrySet()) {
            nvps.add(new BasicNameValuePair(ent.getKey(), ent.getValue()));
        }
        HttpResponse httpResponse = null;
        try {
            method.setEntity(new UrlEncodedFormEntity(nvps, "utf-8"));
            httpResponse = client.execute(method);
        } catch (Exception e) {
            logger.error("httpclient execute get failed!url:" + url, e);
        }
        int statusCode = httpResponse.getStatusLine().getStatusCode();
        if (statusCode == HttpStatus.SC_OK) {
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(httpResponse.getEntity().getContent()));
                String line = null;
                StringBuilder sb = new StringBuilder();
                while ((line = br.readLine()) != null) {
                    sb.append(line).append("/n");
                }
                logger.info("weixin callback {}", sb.substring(0, sb.length() - 1));
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("httpclient read data failed!url:" + url, e);
            }
        } else {
            logger.error("httpclient response statuscode is " + statusCode + "!url:" + url);
        }
    }

}
