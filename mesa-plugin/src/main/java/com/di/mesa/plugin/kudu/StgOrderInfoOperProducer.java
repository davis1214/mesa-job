/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.di.mesa.plugin.kudu;

import backtype.storm.tuple.Tuple;
import com.di.mesa.common.exception.MesaException;
import com.di.mesa.common.util.DateUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by Davi on 17/8/21.
 * <p>
 * kudu_mesa.stg_order_info
 */
public class StgOrderInfoOperProducer implements KuduOperationsProducer, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(StgOrderInfoOperProducer.class);


    private static final List<String> validOperations = Lists.newArrayList(KuduConfigure.KUDU_OPERATION_UPSERT, KuduConfigure.KUDU_OPERATION_INSERT);

    public static final String SKIP_MISSING_COLUMN_PROP = "skipMissingColumn";
    public static final String SKIP_BAD_COLUMN_VALUE_PROP = "skipBadColumnValue";
    public static final String WARN_UNMATCHED_ROWS_PROP = "skipUnmatchedRows";

    public static final String DEFAULT_OPERATION = KuduConfigure.KUDU_OPERATION_UPSERT;
    public static final boolean DEFAULT_SKIP_MISSING_COLUMN = true;
    public static final boolean DEFAULT_SKIP_BAD_COLUMN_VALUE = true;
    public static final boolean DEFAULT_WARN_UNMATCHED_ROWS = true;

    private KuduTable table;
    private Schema schema;

    private String operation;

    private boolean skipMissingColumn;
    private boolean skipBadColumnValue;
    private boolean warnUnmatchedRows;

    public StgOrderInfoOperProducer() {
    }

    @Override
    public void configure(Map conf) {
        operation = conf.getOrDefault(KuduConfigure.KUDU_OPERATION_PROP, DEFAULT_OPERATION).toString().toLowerCase();
        Preconditions.checkArgument(validOperations.contains(operation), "Unrecognized operation '%s'", operation);

        skipMissingColumn = BooleanUtils.toBoolean(conf.getOrDefault(SKIP_MISSING_COLUMN_PROP, DEFAULT_SKIP_MISSING_COLUMN).toString());
        skipBadColumnValue = BooleanUtils.toBoolean(conf.getOrDefault(SKIP_BAD_COLUMN_VALUE_PROP, DEFAULT_SKIP_BAD_COLUMN_VALUE).toString());
        warnUnmatchedRows = BooleanUtils.toBoolean(conf.getOrDefault(WARN_UNMATCHED_ROWS_PROP, DEFAULT_WARN_UNMATCHED_ROWS).toString());
    }

    @Override
    public void initialize(KuduTable table) {
        this.table = table;
        this.schema = table.getSchema();
    }

    @Override
    public List<Operation> getOperations(Tuple tuple) throws MesaException {
        List<Operation> ops = Lists.newArrayList();
        Operation op = generateOperation();
        PartialRow row = op.getRow();

        for (ColumnSchema col : schema.getColumns()) {
            try {
                coerceAndSet(tuple.getStringByField(col.getName()), col.getName(), col.getType(), row);
            } catch (NumberFormatException e) {
                logOrThrow(skipBadColumnValue, String.format("Raw value couldn't be parsed to type %s for column '%s'", col.getType(), col.getName()), e);
            } catch (Exception e) {
                logOrThrow(skipBadColumnValue, String.format("Failed to create Kudu operation to type %s for column '%s'", col.getType(), col.getName()), e);
            }
        }
        ops.add(op);
        return ops;
    }

    private Operation generateOperation() {
        Operation op;
        switch (operation) {
            case KuduConfigure.KUDU_OPERATION_INSERT:
                op = table.newInsert();
                break;
            case KuduConfigure.KUDU_OPERATION_UPSERT:
                op = table.newUpsert();
                break;
            default:
                throw new MesaException(String.format("Unrecognized operation type '%s' in getOperations(): " + "this should never happen!", operation));
        }
        return op;
    }

    private void coerceAndSet(String rawVal, String colName, Type type, PartialRow row)
            throws NumberFormatException {
        if (StringUtils.isEmpty(rawVal)) {
            rawVal = "0";
        }

        switch (type) {
            case STRING:
                if (rawVal.equals("0")) {
                    row.addString(colName, "");
                } else {
                    row.addString(colName, rawVal);
                }
                break;
            case INT8:
                row.addByte(colName, Byte.parseByte(rawVal));
                break;
            case INT16:
                row.addShort(colName, Short.parseShort(rawVal));
                break;
            case INT32:
                row.addInt(colName, Integer.parseInt(rawVal));
                break;
            case INT64:
                row.addLong(colName, Long.parseLong(rawVal));
                break;
            case BOOL:
                row.addBoolean(colName, Boolean.parseBoolean(rawVal));
                break;
            case FLOAT:
                row.addFloat(colName, Float.parseFloat(rawVal));
                break;
            case DOUBLE:
                row.addDouble(colName, Double.parseDouble(rawVal));
                break;
            case UNIXTIME_MICROS:
                if (rawVal.equals("0")) {
                    row.addLong(colName, 0l);
                } else {
                    row.addLong(colName, DateUtil.parseTime(rawVal));
                }
                break;
            default:
                logger.warn("got unknown type {} for column '{}'-- ignoring this column", type, colName);
        }
    }

    private void logOrThrow(boolean log, String msg, Exception e)
            throws MesaException {
        if (log) {
            logger.warn(msg, e);
        } else {
            throw new MesaException(msg, e);
        }
    }

    @Override
    public void close() {
        this.table = null;
    }
}
