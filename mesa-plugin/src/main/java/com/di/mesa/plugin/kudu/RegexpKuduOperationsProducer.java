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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang.BooleanUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Created by Davi on 17/8/21.
 */
public class RegexpKuduOperationsProducer implements KuduOperationsProducer {
    private static final Logger logger = LoggerFactory.getLogger(RegexpKuduOperationsProducer.class);
    private static final String INSERT = "insert";
    private static final String UPSERT = "upsert";
    private static final List<String> validOperations = Lists.newArrayList(UPSERT, INSERT);

    public static final String PATTERN_PROP = "pattern";
    public static final String ENCODING_PROP = "encoding";
    public static final String DEFAULT_ENCODING = "utf-8";
    public static final String OPERATION_PROP = "operation";
    public static final String SKIP_MISSING_COLUMN_PROP = "skipMissingColumn";
    public static final String SKIP_BAD_COLUMN_VALUE_PROP = "skipBadColumnValue";
    public static final String WARN_UNMATCHED_ROWS_PROP = "skipUnmatchedRows";

    public static final String DEFAULT_OPERATION = UPSERT;
    public static final boolean DEFAULT_SKIP_MISSING_COLUMN = false;
    public static final boolean DEFAULT_SKIP_BAD_COLUMN_VALUE = false;
    public static final boolean DEFAULT_WARN_UNMATCHED_ROWS = true;

    private KuduTable table;
    private Pattern pattern;
    private Charset charset;
    private String operation;
    private boolean skipMissingColumn;
    private boolean skipBadColumnValue;
    private boolean warnUnmatchedRows;

    public RegexpKuduOperationsProducer() {
    }

    @Override
    public void configure(Map conf) {
        String regexp = conf.get(PATTERN_PROP).toString();
        Preconditions.checkArgument(regexp != null, "Required parameter %s is not specified", PATTERN_PROP);
        try {
            pattern = Pattern.compile(regexp);
        } catch (PatternSyntaxException e) {
            throw new IllegalArgumentException(String.format("The pattern '%s' is invalid", regexp), e);
        }
        String charsetName = conf.getOrDefault(ENCODING_PROP, DEFAULT_ENCODING).toString();
        try {
            charset = Charset.forName(charsetName);
        } catch (IllegalArgumentException e) {
            throw new MesaException(String.format("Invalid or unsupported charset %s", charsetName), e);
        }
        operation = conf.getOrDefault(OPERATION_PROP, DEFAULT_OPERATION).toString().toLowerCase();
        Preconditions.checkArgument(validOperations.contains(operation), "Unrecognized operation '%s'", operation);
        skipMissingColumn = BooleanUtils.toBoolean(conf.getOrDefault(SKIP_MISSING_COLUMN_PROP, DEFAULT_SKIP_MISSING_COLUMN).toString());
        skipBadColumnValue = BooleanUtils.toBoolean(conf.getOrDefault(SKIP_BAD_COLUMN_VALUE_PROP, DEFAULT_SKIP_BAD_COLUMN_VALUE).toString());
        warnUnmatchedRows = BooleanUtils.toBoolean(conf.getOrDefault(WARN_UNMATCHED_ROWS_PROP, DEFAULT_WARN_UNMATCHED_ROWS).toString());
    }

    @Override
    public void initialize(KuduTable table) {
        this.table = table;
    }

    @Override
    public List<Operation> getOperations(Tuple tuple) throws MesaException {
        String raw = tuple.getString(0);//new String(tuple.getString(0), charset);

        Matcher m = pattern.matcher(raw);
        boolean match = false;
        Schema schema = table.getSchema();
        List<Operation> ops = Lists.newArrayList();
        while (m.find()) {
            match = true;
            Operation op = generateOperation();
            PartialRow row = op.getRow();
            for (ColumnSchema col : schema.getColumns()) {
                try {
                    coerceAndSet(m.group(col.getName()), col.getName(), col.getType(), row);
                } catch (NumberFormatException e) {
                    logOrThrow(skipBadColumnValue, String.format("Raw value '%s' couldn't be parsed to type %s for column '%s'", raw, col.getType(), col.getName()), e);
                } catch (IllegalArgumentException e) {
                    logOrThrow(skipMissingColumn, String.format("Column '%s' has no matching group in '%s'", col.getName(), raw), e);
                } catch (Exception e) {
                    throw new MesaException("Failed to create Kudu operation", e);
                }
            }
            ops.add(op);
        }
        if (!match && warnUnmatchedRows) {
            logger.warn("Failed to match the pattern '{}' in '{}'", pattern, raw);
        }
        return ops;
    }

    private Operation generateOperation() {
        Operation op;
        switch (operation) {
            case UPSERT:
                op = table.newUpsert();
                break;
            case INSERT:
                op = table.newInsert();
                break;
            default:
                throw new MesaException(
                        String.format("Unrecognized operation type '%s' in getOperations(): " + "this should never happen!", operation));
        }
        return op;
    }

    /**
     * Coerces the string `rawVal` to the type `type` and sets the resulting
     * value for column `colName` in `row`.
     *
     * @param rawVal  the raw string column value
     * @param colName the name of the column
     * @param type    the Kudu type to convert `rawVal` to
     * @param row     the row to set the value in
     * @throws NumberFormatException if `rawVal` cannot be cast as `type`.
     */
    private void coerceAndSet(String rawVal, String colName, Type type, PartialRow row)
            throws NumberFormatException {
        switch (type) {
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
            case BINARY:
                row.addBinary(colName, rawVal.getBytes(charset));
                break;
            case STRING:
                row.addString(colName, rawVal);
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
                row.addLong(colName, Long.parseLong(rawVal));
                break;
            default:
                logger.warn("got unknown type {} for column '{}'-- ignoring this column",
                        type, colName);
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
