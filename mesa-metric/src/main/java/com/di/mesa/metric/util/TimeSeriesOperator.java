package com.di.mesa.metric.util;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.di.mesa.metric.common.OpenTSDBException;
import com.di.mesa.metric.model.DataPoint;

public class TimeSeriesOperator {

	private static final String API_PUT_METHOD = "/api/put";
	private static String API_QUERY_METHOD = "/api/query/";

	private final static String OPENTSDB_HOST = "10.1.8.23";
	private final static String OPENTSDB_URL = "http://" + OPENTSDB_HOST + ":4242";

	public static void storeTimePoint(String urlString, DataPoint dataPoint) throws OpenTSDBException {
		ArrayList<DataPoint> point = new ArrayList<DataPoint>();
		point.add(dataPoint);
		storeTimePoints(urlString, point);
	}

	public static void storeTimePoints(String urlString, List<DataPoint> dataPoints) throws OpenTSDBException {
		int responseCode = 0;
		urlString = urlString + API_PUT_METHOD;

		try {
			responseCode = TimeSeriesHelper.insertDataPoints(urlString, dataPoints);
			if (responseCode > 301 || responseCode == 0) {
				throw new OpenTSDBException(responseCode, urlString, dataPoints.toString());
			}
		} catch (IOException e) {
			throw new OpenTSDBException("Error to store the data point list", e);
		}
	}

	public static void storeTimePoint(String urlString, String metric, long epochTime, HashMap<String, String> tags)
			throws OpenTSDBException {
		urlString = urlString + API_PUT_METHOD;
		DataPoint point = new DataPoint();
		point.setMetric(metric);
		point.setTags(tags);
		point.setTimestamp(epochTime);
		storeTimePoint(urlString, point);
	}

	public static String findTsUid(String urlString, HashMap<String, String> tags, String metric) {
		return findTsuid(urlString, tags, metric, TimeSeriesHelper.DEFAULT_START_TIME);
	}

	public static String findTsuid(String urlString, HashMap<String, String> tags, String metric, long startTime) {
		return TimeSeriesHelper.findTsUid(urlString, tags, metric, startTime);
	}

	public static JSONArray retrieveTimeSeries(String urlString, long startEpoch, String metric,
			HashMap<String, String> tags) throws OpenTSDBException {
		long endEpoch = System.currentTimeMillis() / 1000;
		return retrieveTimeSeriesPost(urlString, startEpoch, endEpoch, metric, tags);
	}

	public static JSONArray retrieveTimeSeries(String urlString, long startEpoch, long endEpoch, String metric,
			Map<String, String> tags) throws OpenTSDBException {
		return retrieveTimeSeriesPost(urlString, startEpoch, endEpoch, metric, tags);
	}

	public static JSONObject retrieveTimeSeries(String urlString, long startEpoch, long endEpoch, String metric,
			Map<String, String> tags, boolean showTSUIDs) {
		return retrieveTimeSeriesGet(urlString, startEpoch, endEpoch, metric, tags, showTSUIDs);
	}

	private static JSONArray retrieveTimeSeriesPost(String urlString, long startEpoch, long endEpoch, String metric,
			Map<String, String> tags) throws OpenTSDBException {

		urlString = urlString + API_QUERY_METHOD;
		String result = "";

		try {
			HttpURLConnection httpConnection = TimeSeriesHelper.openHTTPConnectionPOST(urlString);
			OutputStreamWriter wr = new OutputStreamWriter(httpConnection.getOutputStream());

			JSONObject mainObject = new JSONObject();
			mainObject.put("start", startEpoch);
			mainObject.put("end", endEpoch);
			mainObject.put("showSummary", true);
			mainObject.put("showTSUIDs", true);
			mainObject.put("showQuery", true);
			mainObject.put("globalAnnotations", true);
			mainObject.put("noAnnotations", true);

			JSONArray queryArray = new JSONArray();

			JSONObject queryParams = new JSONObject();
			queryParams.put("aggregator", "avg");
			queryParams.put("metric", metric);

			queryArray.add(queryParams);
			// queryArray.put(queryParams);

			if (tags != null) {
				JSONObject queryTags = new JSONObject();

				Iterator<Entry<String, String>> entries = tags.entrySet().iterator();
				while (entries.hasNext()) {
					@SuppressWarnings("rawtypes")
					Map.Entry entry = (Map.Entry) entries.next();
					queryTags.put((String) entry.getKey(), (String) entry.getValue());
				}

				queryParams.put("tags", queryTags);
			}

			mainObject.put("queries", queryArray);
			String queryString = mainObject.toString();

			System.out.println("query: " + queryString);

			wr.write(queryString);
			wr.flush();
			wr.close();

			result = TimeSeriesHelper.readHttpResponse(httpConnection);

		} catch (IOException e) {
			throw new OpenTSDBException("Unable to connect to server", e);
		} catch (JSONException e) {
			throw new OpenTSDBException("Error on request data", e);
		}

		return TimeSeriesHelper.makeResponseJSONArray(result);
	}

	public static String retrieveTimeSeriesWithPostMethod(String urlString, long startEpoch, long endEpoch,
			String metric, Map<String, String> tags) throws OpenTSDBException {

		urlString = urlString + API_QUERY_METHOD;
		String result = "";

		try {
			HttpURLConnection httpConnection = TimeSeriesHelper.openHTTPConnectionPOST(urlString);
			OutputStreamWriter wr = new OutputStreamWriter(httpConnection.getOutputStream());

			JSONObject mainObject = new JSONObject();
			mainObject.put("start", startEpoch);
			mainObject.put("end", endEpoch);
			mainObject.put("showSummary", false);
			mainObject.put("showTSUIDs", false);
			mainObject.put("showQuery", false);
			mainObject.put("globalAnnotations", false);
			mainObject.put("noAnnotations", true);

			JSONArray queryArray = new JSONArray();

			JSONObject queryParams = new JSONObject();
			queryParams.put("aggregator", "sum");
			queryParams.put("metric", metric);

			queryArray.add(queryParams);
			// queryArray.put(queryParams);

			if (tags != null) {
				JSONObject queryTags = new JSONObject();

				Iterator<Entry<String, String>> entries = tags.entrySet().iterator();
				while (entries.hasNext()) {
					@SuppressWarnings("rawtypes")
					Map.Entry entry = (Map.Entry) entries.next();
					queryTags.put((String) entry.getKey(), (String) entry.getValue());
				}

				queryParams.put("tags", queryTags);
			}

			mainObject.put("queries", queryArray);
			String queryString = mainObject.toString();

			System.out.println("query: " + queryString);

			wr.write(queryString);
			wr.flush();
			wr.close();

			result = TimeSeriesHelper.readHttpResponse(httpConnection);

		} catch (IOException e) {
			throw new OpenTSDBException("Unable to connect to server", e);
		} catch (JSONException e) {
			throw new OpenTSDBException("Error on request data", e);
		}

		return result;
	}

	private static JSONObject retrieveTimeSeriesGet(String urlString, long startEpoch, long endEpoch, String metric,
			Map<String, String> tags, boolean showTSUIDs) {

		urlString = urlString + API_QUERY_METHOD;
		String result = "";

		StringBuilder builder = new StringBuilder();

		builder.append("?start=");
		builder.append(startEpoch);
		builder.append("&end=");
		builder.append(endEpoch);
		builder.append("&show_tsuids=");
		builder.append(showTSUIDs);
		builder.append("&m=sum:");
		builder.append(metric);

		if (tags != null) {
			builder.append("{");

			Iterator<Entry<String, String>> entries = tags.entrySet().iterator();
			while (entries.hasNext()) {
				@SuppressWarnings("rawtypes")
				Map.Entry entry = (Map.Entry) entries.next();
				builder.append((String) entry.getKey());
				builder.append("=");
				builder.append((String) entry.getValue());
				if (entries.hasNext()) {
					builder.append(",");
				}
			}
			builder.append("}");
		}

		try {
			HttpURLConnection httpConnection = TimeSeriesHelper.openHTTPConnectionGET(urlString + builder.toString());
			result = TimeSeriesHelper.readHttpResponse(httpConnection);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (OpenTSDBException e) {
			e.printStackTrace();
			result = String.valueOf(e.responseCode);
		}
		return TimeSeriesHelper.makeResponseJSONObject(result);
	}

	// public static double retrieveTimeSeriesValue(RequestContent
	// requestContent) throws OpenTSDBException, JSONException {
	// JSONArray array = TimeSeriesOperator.retrieveTimeSeries(OPENTSDB_URL,
	// requestContent.getStartEpoch(),
	// requestContent.getEndEpoch(), requestContent.getMetric(),
	// requestContent.getTags());
	//
	// JSONObject object = array.getJSONObject(0);
	// JSONObject data = object.getJSONObject("dps");
	// System.out.println("result:" + object.toString());
	//
	// System.out.println("array:\n" + array.toString());
	//
	// AtomicDouble sum = new AtomicDouble(0l);
	// AtomicLong count = new AtomicLong(0l);
	//
	// Set<String> keySet = data.keySet();
	// for (String key : keySet) {
	// count.addAndGet(1l);
	// Integer value = (Integer) data.get(key);
	// sum.getAndAdd(value);
	// }
	// return sum.get() / count.get();
	// }

}
