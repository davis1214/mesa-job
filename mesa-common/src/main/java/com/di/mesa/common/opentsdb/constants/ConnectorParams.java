package com.di.mesa.common.opentsdb.constants;

public interface ConnectorParams {

	public static final String TYPE_PARAM = "type";
	public static final String OPER_PARAM = "oper";

	public static final String ALARM_ID_PARAM = "alarmId";

	public static final String ID_PARAM = "id";

	public static final String ALARM_PARAM = "alarm";
	public static final String MONITOR_PARAM = "monitor";

	public static final String EXECUTOR_ID_PARAM = "executorId";
	public static final String ACTION_PARAM = "action";
	public static final String JOBID_PARAM = "jobid";
	public static final String EXECUTE_PARAM = "execute";

	public static final String EXECID_PARAM = "execid";
	public static final String SHAREDTOKEN_PARAM = "token";
	public static final String USER_PARAM = "user";

	public static final String START_TYPE = "start";
	public static final String SUSPEND_TYPE = "suspend";
	public static final String RESUME_TYPE = "resume";
	public static final String QUERY_TYPE = "query";
	public static final String KILL_TYPE = "kill";
	public static final String STATUS_TYPE = "status";
	public static final String RESTART_TYPE = "restart";

	public static final String RESPONSE_DESC = "desc";
	public static final String RESPONSE_RESULT = "result";
	public static final String RESPONSE_NOTFOUND = "notfound";
	public static final String RESPONSE_ERROR = "error";
	public static final String RESPONSE_SUCCESS = "success";
	public static final String RESPONSE_ALIVE = "alive";
	public static final String RESPONSE_UPDATETIME = "lasttime";
	public static final String RESPONSE_UPDATED_FLOWS = "updated";

	public static final int NODE_NAME_INDEX = 0;
	public static final int NODE_STATUS_INDEX = 1;
	public static final int NODE_START_INDEX = 2;
	public static final int NODE_END_INDEX = 3;

	public static final String UPDATE_TIME_LIST_PARAM = "updatetime";
	public static final String EXEC_ID_LIST_PARAM = "executionId";

	public static final String STATS_MAP_METRICRETRIEVALMODE = "useStats";
	public static final String STATS_MAP_STARTDATE = "from";
	public static final String STATS_MAP_ENDDATE = "to";
	public static final String STATS_MAP_REPORTINGINTERVAL = "interval";
	public static final String STATS_MAP_CLEANINGINTERVAL = "interval";
	public static final String STATS_MAP_EMITTERNUMINSTANCES = "numInstances";

	public static final String SYNCHRONOUS_MODE_SYNC = "sync";
	public static final String SYNCHRONOUS_MODE_ASYNC = "async";
	
	
	

}
