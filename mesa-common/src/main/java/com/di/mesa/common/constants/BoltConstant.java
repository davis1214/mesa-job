package com.di.mesa.common.constants;

public class BoltConstant {

	public static final int PV = 1;
	public static final int SUM = 2;
	public static final int AVG = 3;
	public static final int MAX = 4;
	public static final int MIN = 5;
	public static final int UV = 6;
	public static final int MAP_PV = 7;
	public static final int MAP_SUM = 8;
	public static final int MAP_AVG = 9;
	public static final int MAP_MAX = 10;
	public static final int MAP_MIN = 11;

	public static final String TAG_COMPUTE_TYPE = "ctype";
	public static final String _BLANK = " ";
	public static final String _NULL = "";
	public static final String _NULL_Str = "null";
	public static final String _COMMA = ",";
	public static final String _SEMICOLON = ":";

	public static final String DEFALUT_BOLT_EMIT_NAME = "1";

	public static final String OPEN_TSDB_URL = "http://localhost:4242";

	public static final String _MAP_KEY_COMPUTE_TOKEN = "@";

	public static final String _UNDER_LINE = "_";

	public static final String SEPATAROT = "-_-";
	public static final String DOUBLE_POINT = "..";
	public static final String TRIPLE_POINT = "...";

	public static final String TICK_SPOUT_NAME = "tickSpout";
	// public static final String ES_STORE_TIMER = "es_store_interval";

	public static final String Cost = "cost";
	public static final String Count = "count";
	public static final String PurgeCost = "purgeCost";
	public static final String CommitCost = "commitCost";
	public static final String UpdateCost = "updateCost";

	public static final String AllTuple = "allTuple";
	public static final String AllProcTuple = "allProcTuple";
	public static final String RequestTimeParseCost = "requestTimeParseCost";
	public static final String TupleCost = "tupleCost";
	public static final String PartternMatchCost = "partternMatchCost";
	public static final String ItemFilterCost = "ItemFilterCost";
	public static final String Tree = "tree";

	/**
	 * 冒号 semicolon
	 */
	public static final int SEMICOLON = 1;

	/**
	 * 空格 blank
	 */
	public static final int BLANK = 2;
	/**
	 * phpError
	 */
	public static final int PHPERROR = 3;

	/**
	 * traceLog
	 */
	public static final int TRACELOG = 4;

	/**
	 * 日志过期时间 计算方法:aggreBolt emit间隔 + aggreResult isReady 时间 (舍弃该方案)
	 */
	public static final int EXPIREDTIME = 40;

	public static final String Task_Prop = "taskProp";
	public static final String Monitor_Items = "monitorItems";

}
