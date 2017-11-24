package com.di.mesa.metric.util;


import com.di.mesa.metric.common.AlarmConstants;

public class DimensionUtils {

	public static String computeType2String(int type) {
		switch (type) {
		case AlarmConstants.ALARM_INDEX_COUNT:
			return "pv";
		case AlarmConstants.ALARM_INDEX_SUM:
			return "sum";
		case AlarmConstants.ALARM_INDEX_AVG:
			return "avg";
		case AlarmConstants.ALARM_INDEX_MAX:
			return "max";
		case AlarmConstants.ALARM_INDEX_MIN:
			return "min";
		case AlarmConstants.ALARM_INDEX_UV:
			return "uv";
		case AlarmConstants.ALARM_INDEX_MAP_PV:
			return "mappv";
		case AlarmConstants.ALARM_INDEX_MAP_SUM:
			return "mapsum";
		case AlarmConstants.ALARM_INDEX_MAP_AVG:
			return "mapavg";
		case AlarmConstants.ALARM_INDEX_MAP_MAX:
			return "mapmax";
		case AlarmConstants.ALARM_INDEX_MAP_MIN:
			return "mapmin";
		default:
			return "unknown";
		}
	}

	public static int computeType2Integer(String computeType) {
		switch (computeType) {
		case "pv":
			return AlarmConstants.ALARM_INDEX_COUNT;
		case "sum":
			return AlarmConstants.ALARM_INDEX_SUM;
		case "avg":
			return AlarmConstants.ALARM_INDEX_AVG;
		case "max":
			return AlarmConstants.ALARM_INDEX_MAX;
		case "min":
			return AlarmConstants.ALARM_INDEX_MIN;
		case "uv":
			return AlarmConstants.ALARM_INDEX_UV;
		case "mappv":
			return AlarmConstants.ALARM_INDEX_MAP_PV;
		case "mapsum":
			return AlarmConstants.ALARM_INDEX_MAP_SUM;
		case "mapavg":
			return AlarmConstants.ALARM_INDEX_MAP_AVG;
		case "mapmax":
			return AlarmConstants.ALARM_INDEX_MAP_MAX;
		case "mapmin":
			return AlarmConstants.ALARM_INDEX_MAP_MIN;
		default:
			return 0;
		}
	}

}
