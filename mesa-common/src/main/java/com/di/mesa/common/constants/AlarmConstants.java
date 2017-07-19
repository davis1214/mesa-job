package com.di.mesa.common.constants;

import java.text.SimpleDateFormat;

public class AlarmConstants {

	public static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static final String SCHEDULE_JOB_STATUS_RUNNING = "1";
	public static final String SCHEDULE_JOB_STATUS_NOT_RUNNING = "0";

	public static final String SCHEDULE_JOB_CONCURRENT_IS = "1";
	public static final String SCHEDULE_JOB_CONCURRENT_NOT = "0";

	public static final String ALARM_TYPE_WEIXIN = "wx";
	public static final String ALARM_TYPE_PHONE = "phone";

	// 都好
	public static final String _COMMA = ",";

	// 状态 1:active，0:inactive
	public static final int ALARM_STATUS_ACTIVE = 1;
	public static final int ALARM_STATUS_INACTIVE = 0;

	// 节点类型 1:监控引擎节点，2:告警引擎节点
	public static final int ALARM_TYPE_ALARM_JOB = 2;
	public static final int ALARM_TYPE_MONITOR_JOB = 1;

	// 1:Topic 监控统计延迟情况
	public static final int ALARM_TYPE_TOPIC = 1;
	// 2:Data 统计后的数据是否生成或者波动
	public static final int ALARM_TYPE_DATA = 2;
	// 3:State 服务状态
	public static final int ALARM_TYPE_STATE = 3;

	// 1:告警，
	public static final int ALARM_ON = 1;
	// 0:不告警
	public static final int ALARM_OFF = 0;

	// 告警处理状态 0:未处理，1:已处理
	public static final int ALARM_STATUS_UNPROCESS = 0;
	public static final int ALARM_PROCESS_PROCESSED = 1;

	public static final String ALARM_APP_INSTANCE = "scheduleJob";

	// 任务在数据库状态 0:未提交，1:正在运行，2:暂停，3:失败，4:删除
	public static final int ALARM_STATUS_UNSUBMIT = 0;
	public static final int ALARM_STATUS_RUNNING = 1;
	public static final int ALARM_STATUS_SUSPEND = 2;
	public static final int ALARM_STATUS_FAILED = 3;
	public static final int ALARM_STATUS_KILLED = 4;

	// 告警状态 0:未处理，1:已处理
	public static final int ALARM_STATUS_UNPROCESSED = 0;
	public static final int ALARM_STATUS_PROCESSED = 1;

	// Slide_Type int 1:上下滑动，2:上升，3:下降
	public static final int ALARM_SLIDE_TYPE_UP_DOWN = 1;
	public static final int ALARM_SLIDE_TYPE_UP = 2;
	public static final int ALARM_SLIDE_TYPE_DOWN = 3;

	// 告警类型
	// 1、大于；2、大于等于；3、小于；4、小于等于；5、和上一次差值大于；
	// 6、和上一次差值大于等于；7、和上一次差值小于；8、和上一次差值小于等于；
	// 9、同比；10、环比；11、最大值；12、最小值；13、连续同步
	public static final int ALARM_TYPE_OLDER = 1;
	public static final int ALARM_TYPE_OLDER_EQUAL = 2;
	public static final int ALARM_TYPE_LOWER = 3;
	public static final int ALARM_TYPE_LOWER_EQUAL = 4;
	public static final int ALARM_TYPE_OLDER_THAN_LAST = 5;
	public static final int ALARM_TYPE_OLDER_EQUAL_THAN_LAST = 6;
	public static final int ALARM_TYPE_LOWER_THAN_LAST = 7;
	public static final int ALARM_TYPE_LOWER_EQUAL_THAN_LAST = 8;
	public static final int ALARM_TYPE_COMPARED_RATION = 9; // 同比
	public static final int ALARM_TYPE_SUROUND_RATION = 10;// 环比
	public static final int ALARM_TYPE_MAX_VALUE = 11;// 最大值
	public static final int ALARM_TYPE_MIN_VALUE = 12;// 最小值
	public static final int ALARM_TYPE_CONTINOUS_COMPARED_RATION = 13;

	public static final String ALARM_TYPE_COMPARED_RATION_STRING = "同比"; // 同比
	public static final String ALARM_TYPE_SUROUND_RATION_STRING = "环比";// 环比
	public static final String NULL_STRING = "";// 环比

	// 告警监控指标 1 PV 2 SUM 3 AVG 4 MAX 5 MIN 6 UV 7 MAP_PV 8 MAP_SUM 9 MAP_AVG 10
	// MAP_MAX 11 MAP_MIN' 12 TOPN 13 Mutiple（指标间的计算）
	public static final int ALARM_INDEX_COUNT = 1;
	public static final int ALARM_INDEX_SUM = 2;
	public static final int ALARM_INDEX_AVG = 3;
	public static final int ALARM_INDEX_MAX = 4;
	public static final int ALARM_INDEX_MIN = 5;
	public static final int ALARM_INDEX_UV = 6;
	public static final int ALARM_INDEX_MAP_PV = 7;
	public static final int ALARM_INDEX_MAP_SUM = 8;
	public static final int ALARM_INDEX_MAP_AVG = 9;
	public static final int ALARM_INDEX_MAP_MAX = 10;
	public static final int ALARM_INDEX_MAP_MIN = 11;
	public static final int ALARM_INDEX_TOPN = 12;
	public static final int ALARM_INDEX_MUTIPLE = 13;

	public static final int NO_DATA_FOUND = -1;
	public static final int ZERO_DATA_FOUND = 0;
	public static final int DATA_FOUND = 1;
	public static final int NO_DATA_ENEITY_FOUND = -2;

	public static final int ALARM_VALID = 1;
	public static final int ALARM_INVALID = 0;

	public static final String ENV_TYPE_TEST = "test";
	public static final String ENV_TYPE_PROD = "prod";

	public static final String ALARM_CONN_TYPE_AND = "and";

	public static final String ALARM_CONN_TYPE_OR = "or";
	
	public static String getAlarmType(int alarmType) {
		String type = null;
		switch (alarmType) {
		case ALARM_TYPE_OLDER:
			type = "OLDER";
			break;
		case ALARM_TYPE_OLDER_EQUAL:
			type = "OLDER_EQUAL";
			break;
		case ALARM_TYPE_LOWER:
			type = "LOWER";
			break;
		case ALARM_TYPE_LOWER_EQUAL:
			type = "LOWER_EQUAL";
			break;
		case ALARM_TYPE_OLDER_THAN_LAST:
			type = "OLDER_THAN_LAST";
			break;
		case ALARM_TYPE_OLDER_EQUAL_THAN_LAST:
			type = "OLDER_EQUAL_THAN_LAST";
			break;
		case ALARM_TYPE_LOWER_THAN_LAST:
			type = "LOWER_THAN_LAST";
			break;
		case ALARM_TYPE_LOWER_EQUAL_THAN_LAST:
			type = "LOWER_EQUAL_THAN_LAST";
			break;
		case ALARM_TYPE_COMPARED_RATION:
			type = "COMPARED_RATION";
			break;
		case ALARM_TYPE_SUROUND_RATION:
			type = "SUROUND_RATION";
			break;
		case ALARM_TYPE_MAX_VALUE:
			type = "MAX_VALUE";
			break;

		case ALARM_TYPE_MIN_VALUE:
			type = "MIN_VALUE";
			break;
		case ALARM_TYPE_CONTINOUS_COMPARED_RATION:
			type = "CONTINOUS_COMPARED_RATION";
			break;
		default:
			type = "OTHER";
			break;
		}

		return type;

	}

}
