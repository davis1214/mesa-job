package com.di.mesa.metric.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.di.mesa.metric.model.AlarmConcerner;
import com.di.mesa.metric.util.Sig;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhoneMsgJob implements MsgJob{

	private final Logger logger = LoggerFactory.getLogger(PhoneMsgJob.class);
	
	private  static String priUrl = "http://idc02-im-message-vip00/message/send";
	
	@Override
	public void sendMsg(AlarmConcerner alarmConcerners, String content) throws IOException {
		sendTelMsg(priUrl, alarmConcerners.getTellers(), content);
	}

	private void sendTelMsg(String url, List<String> tels, String content) throws IOException {

		HttpPost httppost = new HttpPost(url);
		ResponseHandler<String> responseHandler = new BasicResponseHandler();
		HttpClient httpclient = new DefaultHttpClient();
		String response = null;

		try {
			for (String tel : tels) {
				String sig = Sig.strToHash(String.format("appkey=%s&mobile=%s", "80bhHzPu", tel));
//				logger.info(sig);
				List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>(2);
				nameValuePairs.add(new BasicNameValuePair("number", tel));
				nameValuePairs.add(new BasicNameValuePair("sp", "gd"));
				nameValuePairs.add(new BasicNameValuePair("batch", "1"));
				nameValuePairs.add(new BasicNameValuePair("msg", content));
				nameValuePairs.add(new BasicNameValuePair("appId", "bjShuiJing"));
				nameValuePairs.add(new BasicNameValuePair("sig", sig));

				httppost.setEntity(new UrlEncodedFormEntity(nameValuePairs, HTTP.UTF_8));

				response = httpclient.execute(httppost, responseHandler);
			}
		}catch (Exception e){
			logger.error(e.getMessage(), e);
		}

		httpclient.getConnectionManager().shutdown();
	}

	public static void main(String[] args){

		try {
			PhoneMsgJob phoneMsgJob = new PhoneMsgJob();
			List<String> list = new LinkedList<>();
			list.add("18701100862");
			phoneMsgJob.sendTelMsg(priUrl, list, "345");
		}catch (Exception e){
			System.out.println(e.getMessage());
		}

	}

}
