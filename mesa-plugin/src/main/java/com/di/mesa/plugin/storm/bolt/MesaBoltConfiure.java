package com.di.mesa.plugin.storm.bolt;

import java.util.ArrayList;
import java.util.List;

public class MesaBoltConfiure {

    public static final String CONFIG_FILE_NAME = "server.properties";
    public static final long MINUTE = 60 * 1000;
    public static final long SECOND = 1000;
    public static final long MAX_DAY = 100000000l;

    public static final long DEFAULT_DAY = 20170101;

    public static final String TICK_SPOUT_NAME = "TICK_SPOUT";

    // 是否启用白名单
    public static final String BUSI_WHITE_LIST_ENABLED = "busi.white.list.enabled";

    public static final String SHOULD_RECORD_METRIC_TO_OPENTSDB = "busi.record.monitor.to.opentsdb";
    public static final String OPENTSDB_URL = "busi.record.monitor.opentsdb.url";

    //add
    public static final List<String> WhiteNameList = new ArrayList<String>() {{
        add("10000030049");
        add("10000081292");
        add("10000081293");
    }};

}
