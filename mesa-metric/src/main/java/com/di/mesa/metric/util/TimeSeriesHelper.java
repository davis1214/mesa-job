package com.di.mesa.metric.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.di.mesa.metric.common.HttpTypeEnum;
import com.di.mesa.metric.common.OpenTSDBException;
import com.di.mesa.metric.model.DataPoint;

public class TimeSeriesHelper {

	protected static long DEFAULT_START_TIME = 1420088400L;// 1 January, 2015
															// 00:00:00

	protected static String findTsuid(String urlString, HashMap<String, String> tags, String metric) {
		return findTsUid(urlString, tags, metric, DEFAULT_START_TIME);
	}

	protected static String findTsUid(String urlString, HashMap<String, String> tags, String metric, long startTime) {
		// long endTime = startTime + 1;
		long endTime = (System.currentTimeMillis() / 1000 - 60);
		JSONObject results = null;
		results = TimeSeriesOperator.retrieveTimeSeries(urlString, startTime, endTime, metric, tags, true);
		String ret = null;
		try {
			JSONArray tsuidsArray = results.getJSONArray("tsuids");
			ret = tsuidsArray.getString(0);
		} catch (JSONException e) {
			e.printStackTrace();
		}

		return ret;
	}

	protected static int insertDataPoints(String urlString, List<DataPoint> points) throws IOException {
		int code = 0;
		Gson gson = new Gson();

		HttpURLConnection httpConnection = TimeSeriesHelper.openHTTPConnectionPOST(urlString);
		OutputStreamWriter wr = new OutputStreamWriter(httpConnection.getOutputStream());

		String json = gson.toJson(points);

		System.out.println("put:" + json);
		wr.write(json);
		wr.flush();
		wr.close();

		code = httpConnection.getResponseCode();

		httpConnection.disconnect();

		return code;
	}

	protected static HttpURLConnection openHTTPConnectionGET(String urlString) throws MalformedURLException,
			IOException {
		return openHTTPConnection(urlString, HttpTypeEnum.GET);
	}

	protected static HttpURLConnection openHTTPConnectionPOST(String urlString) throws MalformedURLException,
			IOException {
		return openHTTPConnection(urlString, HttpTypeEnum.POST);
	}

	protected static HttpURLConnection openHTTPConnectionPUT(String urlString) throws MalformedURLException,
			IOException {
		return openHTTPConnection(urlString, HttpTypeEnum.PUT);
	}

	protected static HttpURLConnection openHTTPConnectionDELETE(String urlString) throws MalformedURLException,
			IOException {
		return openHTTPConnection(urlString, HttpTypeEnum.DELETE);
	}

	private static HttpURLConnection openHTTPConnection(String urlString, HttpTypeEnum verb)
			throws MalformedURLException, IOException {
		URL url = null;
		HttpURLConnection conn = null;

		url = new URL(urlString);
		conn = (HttpURLConnection) url.openConnection();

		switch (verb) {
		case POST:
			conn.setRequestMethod("POST");
			conn.setDoOutput(true);
			conn.setDoInput(true);
			break;
		case PUT:
			conn.setRequestMethod("PUT");
			conn.setDoOutput(true);
			break;
		case DELETE:
			conn.setRequestMethod("DELETE");
			break;
		default:
			conn.setRequestMethod("GET");
			break;
		}

		conn.setRequestProperty("Accept", "application/json");
		conn.setRequestProperty("Content-type", "application/json");

		return conn;
	}

	protected static String readHTTPConnection(HttpURLConnection conn) throws UnsupportedEncodingException, IOException {
		StringBuilder sb = new StringBuilder();
		BufferedReader br;

		br = new BufferedReader(new InputStreamReader(conn.getInputStream(), "utf-8"));
		String line = null;
		while ((line = br.readLine()) != null) {
			sb.append(line + "\n");
		}
		br.close();

		return sb.toString();
	}

	protected static String readHttpResponse(HttpURLConnection httpConnection) throws OpenTSDBException, IOException {
		String result = "";
		int responseCode = httpConnection.getResponseCode();

		if (responseCode == 200) {
			result = readHTTPConnection(httpConnection);
		} else if (responseCode == 204) {
			result = String.valueOf(responseCode);
		} else {
			throw new OpenTSDBException(responseCode, httpConnection.getURL().toString(), "");
		}
		httpConnection.disconnect();

		return result;
	}

	protected static JSONObject makeResponseJSONObject(String data) {
		JSONArray product = null;
		JSONObject ret = null;
		try {
			product = JSONArray.parseArray(data);
			ret = (JSONObject) product.get(0);
		} catch (JSONException e) {
			e.printStackTrace();
		}

		return ret;
	}

	protected static JSONArray makeResponseJSONArray(String data) {

		JSONArray array = null;
		try {
			array = JSONArray.parseArray(data);
		} catch (JSONException e) {
			array = new JSONArray();
			array.add(makeResponseJSONObject(data));
		}

		return array;
	}
}
