package azkaban.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

public class DateUtil {

	public static SimpleDateFormat sdf = null;

	public static final String FORMAT_YEAR = "yyyy";
	public static final String FORMAT_MONTH = "yyyy-MM";
	public static final String FORMAT_DAY = "dd";
	public static final String FORMAT_DATE = "yyyy-MM-dd";
	public static final String FORMAT_TIME = "hh:mm:ss";
	public static final String FORMAT_DATETIME = "yyyy-MM-dd HH:mm:ss";
	public static final String FORMAT_DATETIMES = "yyyyMMdd/HHmmss";
	public static final String FORMAT_DATE_STR = "yyyyMMdd";

	private static void setSDF() {
		TimeZone tz = TimeZone.getDefault();
		TimeZone.setDefault(TimeZone.getTimeZone("GMT-0"));
		sdf = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss 'GMT'", Locale.US);
		TimeZone.setDefault(tz);
	}

	public static synchronized long getDateLong(String in) {
		long a = 0;
		if (sdf == null) {
			setSDF();
		}
		try {
			// System.out.println(in);
			a = sdf.parse(in).getTime();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return a;
	}

	public static synchronized String getDateString(long in) {
		if (sdf == null) {
			setSDF();
		}
		return sdf.format(new Date(in));
	}

	public static String StrDate(String format, Date date) {
		SimpleDateFormat ff = new SimpleDateFormat();
		ff.applyPattern(format);
		return ff.format(date);
	}

	/**
	 *
	 * @param date
	 * @return Date
	 */
	public static Date getNextNYear(Date date, int n) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		int year = calendar.get(Calendar.YEAR);
		calendar.set(Calendar.YEAR, year + n);
		return calendar.getTime();
	}

	/**
	 *
	 * @param date
	 * @return Date
	 */
	public static Date getNextNDate(Date date, int n) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		int day = calendar.get(Calendar.DATE);
		calendar.set(Calendar.DATE, day + n);
		return calendar.getTime();
	}

	/**
	 *
	 * @param date
	 * @return Date
	 */
	public static Date getPreviousNDate(Date date, int n) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		int day = calendar.get(Calendar.DATE);
		calendar.set(Calendar.DATE, day - n);
		return calendar.getTime();
	}

	public static Date getFirstDate(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		int minDate = calendar.getActualMinimum(Calendar.DATE);
		calendar.set(Calendar.DATE, minDate);
		return calendar.getTime();
	}

	public static Date getLastDate(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		int maxDate = calendar.getActualMaximum(Calendar.DATE);
		calendar.set(Calendar.DATE, maxDate);
		return calendar.getTime();
	}

	public static Date getCurMonth() {
		return getNextNMonth(new Date(), 0);
	}

	public static Date getNextNMonth(int n) {
		return getNextNMonth(new Date(), n);
	}

	public static Date getNextNMonth(Date date, int n) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(Calendar.DAY_OF_MONTH, 1);
		calendar.add(Calendar.MONTH, n);
		return calendar.getTime();
	}

	public static String getCurMonthStr() {
		return StrDate(FORMAT_MONTH, getCurMonth());
	}

	public static String getNextNMonthStr(int n) {
		return StrDate(FORMAT_MONTH, getNextNMonth(n));
	}

	public static String getNextNMonthStr(Date date, int n) {
		return StrDate(FORMAT_MONTH, getNextNMonth(date, n));
	}

	public static String getDateStr(Date date) {
		return StrDate(FORMAT_DATE, date);
	}

	public static String getCurDateStr() {
		return StrDate(FORMAT_DATE, new Date());
	}

	public static String getCurDateTimeStr() {
		return StrDate(FORMAT_DATETIME, new Date());
	}

	public static Date getPreviousNMonth(Date date, int n) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		int month = calendar.get(Calendar.MONTH);
		calendar.set(Calendar.MONTH, month - n);
		return calendar.getTime();
	}

	public static Date getPreviousNHour(Date date, int n) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		int hour = calendar.get(Calendar.HOUR);
		calendar.set(Calendar.HOUR, hour - n);
		return calendar.getTime();
	}

	public static Date getPreviousNMinute(Date date, int n) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		int minute = calendar.get(Calendar.MINUTE);
		calendar.set(Calendar.MINUTE, minute - n);
		return calendar.getTime();
	}

	public static Date getPreviousNSecond(Date date, int n) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		int second = calendar.get(Calendar.SECOND);
		calendar.set(Calendar.SECOND, second - n);
		return calendar.getTime();
	}

	public static Date getAppointDate(Date date, int day) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(Calendar.DATE, day);
		return calendar.getTime();
	}

	/**
	 * <p>
	 * Title: getTimesnight
	 * </p>
	 * <p>
	 * Description:获得当天24点时间
	 * </p>
	 * 
	 * @return
	 */
	public static long getTimesnight() {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.HOUR_OF_DAY, 24);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTimeInMillis();
	}

	/**
	 * <p>
	 * Title: getTimesnight
	 * </p>
	 * <p>
	 * Description:获得当天0点时间
	 * </p>
	 * 
	 * @return
	 */
	public static long getTimesmorning() {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTimeInMillis();
	}

	public static String dateplus(String from, int n) {
		Date date = StrUtil.string2Date(FORMAT_DATE, from);
		Date d = getPreviousNDate(date, n);
		return StrUtil.date2String(FORMAT_DATE, d);
	}

	public static String dateplus(String from, int n, int type) {
		if (from.length() == 10) {
			from += " 00:00:00";
		}
		Date date = StrUtil.string2Date(FORMAT_DATETIME, from);
		Date d = new Date();
		if (type == Calendar.MONTH) {
			d = getPreviousNMonth(date, n);
		} else if (type == Calendar.HOUR) {
			d = getPreviousNHour(date, n);
		} else if (type == Calendar.MINUTE) {
			d = getPreviousNMinute(date, n);
		} else if (type == Calendar.SECOND) {
			d = getPreviousNSecond(date, n);
		} else {
			d = getPreviousNDate(date, n);
		}
		return StrUtil.date2String(FORMAT_DATETIME, d);
	}

	/**
	 *
	 * @param date1
	 * @param date2
	 * @return
	 */
	public static int diff(String date1, String date2) {
		if (StrUtil.empty(date1) || StrUtil.empty(date2)) {
			return 0;
		}
		Date d1 = StrUtil.string2Date(FORMAT_DATE, date1);
		Date d2 = StrUtil.string2Date(FORMAT_DATE, date2);
		int n = 0;
		while (d1.getTime() != d2.getTime()) {
			if (d1.getTime() < d2.getTime()) {
				d1 = getNextNDate(d1, 1);
				n++;
			} else {
				d1 = getNextNDate(d1, -1);
				n--;
			}
		}
		return n;
	}

	public static Map<String, Integer> getDateDetail(String d) {
		Map<String, Integer> ret = new HashMap<String, Integer>();
		if (d.length() == 10) {
			d += " 00:00:00";
		}
		Date dd = StrUtil.string2Date(FORMAT_DATETIME, d);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(dd);
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH);
		int date = calendar.get(Calendar.DATE);
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		int minute = calendar.get(Calendar.MINUTE);
		int second = calendar.get(Calendar.SECOND);
		int week = calendar.get(Calendar.DAY_OF_WEEK);
		ret.put("year", year);
		ret.put("month", month);
		ret.put("date", date);
		ret.put("hour", hour);
		ret.put("minute", minute);
		ret.put("second", second);
		ret.put("week", week);
		return ret;
	}

	/**
	 *
	 * @param from
	 * @param to
	 * @return
	 */
	public static Map dateScopeMap(String from, String to) {
		Map map = new LinkedHashMap();
		map.put(from, null);
		if (from.length() == 4) {
			int i = 1;
			Date d = StrUtil.string2Date(FORMAT_YEAR, from);
			while (d.getTime() < StrUtil.string2Date(FORMAT_YEAR, to).getTime()) {
				d = getNextNYear(StrUtil.string2Date(FORMAT_YEAR, from), i);
				map.put(StrUtil.date2String(FORMAT_YEAR, d), null);
				i++;
			}
		} else if (from.length() == 7) {
			int i = 1;
			Date d = StrUtil.string2Date("yyyy-MM", from);
			while (d.getTime() < StrUtil.string2Date("yyyy-MM", to).getTime()) {
				d = getPreviousNMonth(StrUtil.string2Date("yyyy-MM", from), -i);
				map.put(StrUtil.date2String("yyyy-MM", d), null);
				i++;
			}
		} else {
			int i = 1;
			Date d = StrUtil.string2Date(FORMAT_DATE, from);
			while (d.getTime() < StrUtil.string2Date(FORMAT_DATE, to).getTime()) {
				d = getNextNDate(StrUtil.string2Date(FORMAT_DATE, from), i);
				map.put(StrUtil.date2String(FORMAT_DATE, d), null);
				i++;
			}
		}
		return map;
	}

	public static List<String> dateScope(String from, String to) {
		List<String> list = new ArrayList<String>();
		list.add(from);
		if (from.length() == 4) {
			int i = 1;
			Date d = StrUtil.string2Date(FORMAT_YEAR, from);
			while (d.getTime() < StrUtil.string2Date(FORMAT_YEAR, to).getTime()) {
				d = getNextNYear(StrUtil.string2Date(FORMAT_YEAR, from), i);
				list.add(StrUtil.date2String(FORMAT_YEAR, d));
				i++;
			}
		} else if (from.length() == 7) {
			int i = 1;
			Date d = StrUtil.string2Date(FORMAT_MONTH, from);
			while (d.getTime() < StrUtil.string2Date(FORMAT_MONTH, to).getTime()) {
				d = getPreviousNMonth(StrUtil.string2Date(FORMAT_MONTH, from), -i);
				list.add(StrUtil.date2String(FORMAT_MONTH, d));
				i++;
			}
		} else {
			int i = 1;
			Date d = StrUtil.string2Date(FORMAT_DATE, from);
			while (d.getTime() < StrUtil.string2Date(FORMAT_DATE, to).getTime()) {
				d = getNextNDate(StrUtil.string2Date(FORMAT_DATE, from), i);
				list.add(StrUtil.date2String(FORMAT_DATE, d));
				i++;
			}
		}
		return list;
	}

	
	public static int getDayOfMonth() {
		Calendar calendar = Calendar.getInstance();
		Date date = new Date();
		calendar.setTime(date);
		int dayOfWeek = calendar.get(Calendar.DAY_OF_MONTH);
		return dayOfWeek;
	}

	public static int getDayOfWeek() {
		Calendar calendar = Calendar.getInstance();
		Date date = new Date();
		calendar.setTime(date);
		int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK) - 1;
		if (dayOfWeek <= 0)
			dayOfWeek = 7;
		return dayOfWeek;
	}

	public static String getStartDateOfWeek() {
		int dayOfWeek = getDayOfWeek();
		Date today = new Date();
		Date date = getNextNDate(today, 1 - dayOfWeek);
		return StrDate(FORMAT_DATE, date);
	}

	public static String getNextStartDateOfWeek() {
		int dayOfWeek = getDayOfWeek();
		Date today = new Date();
		Date date = getNextNDate(today, 1 - dayOfWeek + 7);
		return StrDate(FORMAT_DATE, date);
	}

	public static String getNextBidDate() {
		int dayOfWeek = getDayOfWeek();
		Date today = new Date();
		Date date;
		if (dayOfWeek < 5)
			date = getNextNDate(today, 5 - dayOfWeek);
		else {
			date = getNextNDate(today, 5 - dayOfWeek + 7);
		}
		return StrDate(FORMAT_DATE, date);
	}

	public static String getCurrentBidInternal() {
		int dayOfWeek = getDayOfWeek();
		Date today = new Date();
		Date startDate;
		Date endDate;
		if (dayOfWeek < 5) {
			startDate = getNextNDate(today, 1 - dayOfWeek);
			endDate = getNextNDate(today, 7 - dayOfWeek);
		} else {
			startDate = getNextNDate(today, 1 - dayOfWeek + 7);
			endDate = getNextNDate(today, 7 - dayOfWeek + 7);
		}
		return StrDate(FORMAT_DATE, startDate) + "~" + StrDate(FORMAT_DATE, endDate);
	}

	public static String getCurScribeDateStr() {
		return getCurDateStr().replaceAll("-", "/") + "/";
	}

	public static String getYearStr(Date date) {
		String dateStr = StrDate(FORMAT_YEAR, date);
		return dateStr;
	}

	public static String getMonthStr(Date date) {
		return StrDate(FORMAT_MONTH, date);
	}

	public static String getDayStr(Date date) {
		return StrDate(FORMAT_DAY, date);
	}

	public static String getGisAccountTimeStr(long gpsTimeStamp) {
		Date date = new Date(gpsTimeStamp);
		return StrDate(FORMAT_DATETIMES, date);
	}

	public static long parseGPSTime(String time) {
		if (StrUtil.empty(time)) {
			return 0L;
		}
		String[] unit = time.split("-");
		if (unit == null || unit.length != 6) {
			return 0L;
		}
		return StrUtil.string2Date(FORMAT_DATETIME,
				unit[0] + "-" + unit[1] + "-" + unit[2] + " " + unit[3] + ":" + unit[4] + ":" + unit[5]).getTime();
	}

	public static long getTimesNight(long time) {
		Date date = StrUtil.string2Date("yyyy-MM-dd", getDateStr(new Date(time)));
		return date.getTime() + 1000 * 60 * 60 * 24;
	}

	public static long getTimesHourBegin(long time) {
		Date date = StrUtil.string2Date("yyyy-MM-dd HH", StrDate("yyyy-MM-dd HH", new Date(time)));
		return date.getTime();
	}

	public static long getTimesHourEnd(long time) {
		Date date = StrUtil.string2Date("yyyy-MM-dd HH", StrDate("yyyy-MM-dd HH", new Date(time)));
		return date.getTime() + 1000 * 60 * 60;
	}

	public static long getTimesmorning(long time) {
		//out.println(getDateStr(new Date(time)));
		Date date = StrUtil.string2Date("yyyy-MM-dd", getDateStr(new Date(time)));
		//out.println(date);
		return date.getTime();
	}

	public static long getTimesmorning(String dateStr) {
		Date date = StrUtil.string2Date("yyyyMMdd", dateStr);
		return date.getTime();
	}

	public static long getTimesNight(String dateStr) {
		Date date = StrUtil.string2Date("yyyyMMdd", dateStr);
		return date.getTime() + 1000 * 60 * 60 * 24;
	}

	public static long getTimeEnd(String dayStr, String hour) {
		Date date = StrUtil.string2Date("yyyyMMdd", dayStr);
		if (hour.equals("0"))
			return date.getTime() + 1000 * 60 * 60 * 24;
		else
			return date.getTime() + 1000 * 60 * 60 * Integer.valueOf(hour);
	}

	public static long getTimeStart(String dayStr, String hour) {
		Date date = StrUtil.string2Date("yyyyMMdd", dayStr);
		if (hour.equals("0"))
 			return date.getTime();
		else
			return date.getTime() - 1000 * 60 * 60 * (24 - Integer.valueOf(hour)+1);
	}

	public static String getCurrentDateStr() {
		String date = DateUtil.getCurDateStr();
		return date.replaceAll("-", "/");
	}

	public static String getYesterdayDateStr() {
		Date yesterday = DateUtil.getPreviousNDate(new Date(), 1);
		String date = DateUtil.getDateStr(yesterday);
		return date.replaceAll("-", "/");
	}

	public static String getDateStr(String timestamp) {
		long time = StrUtil.parseLong(timestamp);
		String date = DateUtil.getDateStr(new Date(time));
		return date.replaceAll("-", "/");
	}

	public static String beforeAppointedDate(String dateStr) {
		try {
			Date date = new SimpleDateFormat("yyyy/MM/dd").parse(dateStr);
			Date yesterday = DateUtil.getPreviousNDate(date, 1);
			return DateUtil.getDateStr(yesterday).replaceAll("-", "/");
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String afterAppointedDate(String dateStr) {
		try {
			Date date = new SimpleDateFormat("yyyy/MM/dd").parse(dateStr);
			Date yesterday = DateUtil.getNextNDate(date, 1);
			return DateUtil.getDateStr(yesterday).replaceAll("-", "/");
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static boolean withinAppointedDate(long time, String dateStr) {
		try {
			Date date = new SimpleDateFormat("yyyy/MM/dd").parse(dateStr);
			long start = date.getTime();
			long end = date.getTime() + 1000 * 60 * 60 * 24;
			if (time > start && time <= end) {
				return true;
			}

		} catch (ParseException e) {
			e.printStackTrace();
		}
		return false;
	}

	public static int decideDayWeekly(String paramStrDate) {
		int flag = -1;
		if (paramStrDate == null || paramStrDate.trim().length() == 0) {
			return flag;
		}
		boolean isCurrent = false;
		long minusDay = 0;
		// 定义基准日期：2010-07-04是星期日
		Date baseDate = constructDateByString("2010-07-04 00:00:00");
		Date paramDate = constructDateByString(paramStrDate + " 00:00:00");
		if (paramDate == null) {
			System.err.println("The Date Format Is Not Available.");
			return flag;
		}
		// 基准日期之前的日期
		if (!isCurrent && paramDate.before(baseDate)) {
			minusDay = (baseDate.getTime() - paramDate.getTime()) / (24 * 60 * 60 * 1000);
			// 注意：往前推算的算法
			flag = Integer.parseInt("" + ((7 - minusDay % 7)) % 7);
			// System.out.println("=minusDay of 1 is:=" + minusDay + "=end=");
			isCurrent = true;
		}
		// 基准日期之后的日期
		if (!isCurrent && paramDate.after(baseDate)) {
			minusDay = (paramDate.getTime() - baseDate.getTime()) / (24 * 60 * 60 * 1000);
			flag = Integer.parseInt("" + minusDay % 7);
			// System.out.println("=minusDay of 2 is:=" + minusDay + "=end=");
			isCurrent = true;
		}
		// 就是2010-07-04,基准日期为星期日
		if (!isCurrent) {
			flag = 0;
		}
		return flag;
	}

	public static Date constructDateByString(String paramStr) {
		if (paramStr == null || paramStr.trim().length() == 0) {
			return null;
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date retDate = null;
		try {
			retDate = sdf.parse(paramStr);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return retDate;
	}

	public static Date constructDateByString(String paramStr, String format) {
		if (paramStr == null || paramStr.trim().length() == 0) {
			return null;
		}

		SimpleDateFormat sdf = new SimpleDateFormat(format);
		Date retDate = null;
		try {
			retDate = sdf.parse(paramStr);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return retDate;
	}

	public static String getYearMonthDateStr(Date date) {
		return StrDate(FORMAT_DATE_STR, date);
	}
}
