package com.di.mesa.common.util;

import com.di.mesa.common.constants.BoltConstant;
import jregex.Matcher;
import jregex.Pattern;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.StringTokenizer;

public class RequestTimeUtil {
	private int requestType;
	private String topic;

	private String semicolon;

	private String phpError;

	private String traceLog;

	private SimpleDateFormat defautSdf;
	private Pattern defaultDatePattern;
	private SimpleDateFormat PHP_ERROR_DATE_FORMAT;

	public RequestTimeUtil(int requestType) {
		this.requestType = requestType;
	}

//	private static ThreadLocal<SimpleDateFormat> threadLocal = new ThreadLocal<SimpleDateFormat>() {
//		protected synchronized SimpleDateFormat initialValue() {
//			return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//		}
//	};
	private SimpleDateFormat defaultFormat ;

	/**
	 * ＊ 每个topic对应不同格式数据
	 * 
	 * @param topic
	 */
	public RequestTimeUtil(String topic) {
		this.topic = topic;

		this.semicolon = PropertyUtil.getValueByKey("system.properties", "storm.log.reuqest.time.parser.semicolon");
		this.phpError = PropertyUtil.getValueByKey("system.properties", "storm.log.reuqest.time.parser.phperror");
		this.traceLog = PropertyUtil.getValueByKey("system.properties", "storm.log.request.time.parser.tracelog");
		this.requestType = initRequestType();

		defaultDatePattern = new Pattern("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.*?");
		// defautSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		PHP_ERROR_DATE_FORMAT = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss", Locale.US);

//		try {
//			// defautSdf.parse("2016-09-23 10:59:57.750");//
//			// 预初始化解析(第一次解析时耗时较长，可能影响数据统计)
//			threadLocal.get().parse("2016-09-23 10:59:57.750");// 预初始化解析(第一次解析时耗时较长，可能影响数据统计)
//		} catch (ParseException e) {
//			e.printStackTrace();
//		}
		defaultFormat = new  SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	}

	private int initRequestType() {

		if (semicolon.contains(this.topic)) {
			return BoltConstant.SEMICOLON;
		}
		if (phpError.contains(this.topic)) {
			return BoltConstant.PHPERROR;
		}
		if (traceLog.contains(this.topic)) {
			return BoltConstant.TRACELOG;
		}
		return BoltConstant.BLANK;
	}

	/**
	 * 获取时间 抽取出来一个方法类
	 *
	 * @param line
	 * @return
	 */
	public long getRequestTime(String line) {
		try {

			if (requestType == BoltConstant.PHPERROR) {
				String timeStr = line.substring(line.indexOf("[") + 1, line.indexOf("]"));
				Date parse = PHP_ERROR_DATE_FORMAT.parse(timeStr);
				// 加8个小时
				Long timestamp = parse.getTime() / 1000 + 8 * 3600;
				return timestamp;
			}

			if (requestType == BoltConstant.TRACELOG) {
				StringTokenizer tokenizer = new StringTokenizer(line, BoltConstant._BLANK);
				Long timestamp = Long.parseLong(tokenizer.nextToken()) / 1000;
				return timestamp;
			}

			String requestTime = getRequestStrTIme_highPerformance(line);
			Matcher matcher = defaultDatePattern.matcher(requestTime);
			if (matcher.find()) {
				// Date date = defautSdf.parse(requestTime);
//				Date date = threadLocal.get().parse(requestTime);
				Date date = defaultFormat.parse(requestTime);
				long timestamp = date.getTime() / 1000;
				return timestamp;
			}
		} catch (Exception e) {
			return -1L;
		}
		return -1L;
	}

	public long getRequestTime2(String line) {
		try {
			String requestTime = getRequestStrTIme(line);
			Matcher matcher = defaultDatePattern.matcher(requestTime);
			// if (StringUtils.isNotEmpty(requestTime) && matcher.find()) {
			if (matcher.find()) {
				Date date = defautSdf.parse(requestTime);
				long timestamp = date.getTime() / 1000;
				return timestamp;
			}
		} catch (Exception e) {
			return -1L;
		}
		return -1L;
	}

	private String getRequestStrTIme_highPerformance(String line) {
		String requestTime;

		switch (requestType) {
		case BoltConstant.SEMICOLON:
			String newLine = line.substring(line.indexOf(BoltConstant._SEMICOLON) + 1);
			StringTokenizer tokenizer = new StringTokenizer(newLine, BoltConstant._BLANK);
			requestTime = tokenizer.nextToken() + BoltConstant._BLANK + tokenizer.nextToken();
			break;
		default:
			StringTokenizer defaultTokenizer = new StringTokenizer(line, BoltConstant._BLANK);
			defaultTokenizer.nextToken();
			requestTime = defaultTokenizer.nextToken() + BoltConstant._BLANK + defaultTokenizer.nextToken();
			break;
		}
		return requestTime;
	}

	private String getRequestStrTIme(String line) {
		String requestTime;
		String[] timeArray;
		switch (requestType) {
		case BoltConstant.SEMICOLON:
			String newLine = line.substring(line.indexOf(BoltConstant._SEMICOLON) + 1);
			timeArray = SplitUtil.splitPreserveAllTokens(newLine, BoltConstant._BLANK);
			requestTime = timeArray[0] + BoltConstant._BLANK + timeArray[1];
			break;
		default:
			timeArray = SplitUtil.splitPreserveAllTokens(line, BoltConstant._BLANK);
			requestTime = timeArray[1] + BoltConstant._BLANK + timeArray[2];
			break;
		}
		return requestTime;
	}

	public static void main(String[] args) {
		RequestTimeUtil util = new RequestTimeUtil("www_udc_itemwd");

		for (int j = 0; j < 40; j++) {
			long start = System.currentTimeMillis();
			for (int c = 0; c < 10000; c++) {
				// System.out.println(
				new Date(1000 * util.getRequestTime("NOTICE: 2016-10-11 17:10:01.440 140384216864512" + c));
			}
			System.out.println("cost " + (System.currentTimeMillis() - start));
		}
	}
}