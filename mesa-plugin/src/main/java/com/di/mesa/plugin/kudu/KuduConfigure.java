package com.di.mesa.plugin.kudu;

/**
 * Created by Davi on 17/8/21.
 */
public class KuduConfigure {

    public final static String KUDU_MASTER_ADDRESS = "busi.kudu.master.address";

    public final static String KUDU_TABLE_NAME = "busi.kudu.table.name";
    public final static String KUDU_TABLE_SCHEMA = "busi.kudu.table.schema";
    public static final String KUDU_OPERATION_PROP = "busi.kudu.table.operation";

    public final static String KUDU_BOLT_DECLARED_FIELDS = "busi.kudu.bolt.declared.fields";
    public final static String KUDU_BOLT_STREAMING_ID = "busi.kudu.bolt.streaming.id";

    public final static String KUDU_BOLT_BATCH_SIZE = "busi.kudu.bolt.batch.size";
    public final static String KUDU_BOLT_TIME_OUT = "busi.kudu.bolt.time.out";
    public final static String KUDU_BOLT_IGNORE_DUPLICATE_ROWS = "busi.kudu.bolt.ignore.duplicate.rows";

    public final static String KUDU_BOLT_PRODUCER = "busi.kudu.bolt.producer";



    //value
    public static final String KUDU_OPERATION_INSERT = "insert";
    public static final String KUDU_OPERATION_UPSERT = "upsert";


}
