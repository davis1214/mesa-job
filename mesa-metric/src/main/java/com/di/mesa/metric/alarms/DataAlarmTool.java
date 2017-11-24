package com.di.mesa.metric.alarms;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.di.mesa.metric.common.AlarmConstants;
import com.di.mesa.metric.common.CommonConstant;
import com.di.mesa.metric.model.ComputeDimension;
import com.di.mesa.metric.model.MAlarm;
import com.di.mesa.metric.model.MAlarmCondition;
import com.di.mesa.metric.util.DimensionUtils;
import com.di.mesa.metric.util.ExprCaculateUtil;
import com.di.mesa.metric.util.StrUtil;
import com.di.mesa.metric.client.ShuffledOpentsdbClient;
import com.di.mesa.metric.client.request.Query;
import com.di.mesa.metric.client.request.QueryBuilder;
import com.di.mesa.metric.client.request.SubQueries;
import com.di.mesa.metric.client.util.ExecutorApiClient;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

public class DataAlarmTool {

    private ShuffledOpentsdbClient opentsdbDao;
    private SimpleDateFormat simpleDateFormat;
    public Logger logger;

    public String getToken(int computeType) {
        if (computeType == AlarmConstants.ALARM_TYPE_COMPARED_RATION) {
            return AlarmConstants.ALARM_TYPE_COMPARED_RATION_STRING;
        } else if (computeType == AlarmConstants.ALARM_TYPE_SUROUND_RATION) {
            return AlarmConstants.ALARM_TYPE_SUROUND_RATION_STRING;
        }
        return AlarmConstants.NULL_STRING;
    }

    protected Double getDouble(double value) {
        return (double) Math.round(value * 100) / 100;
    }

    public int getPeriod(int period) {
        return (int) Math.round(period / 60);
    }

    public String getMutipleComputeData(MAlarm malarm, long startTime, long endTime) throws IOException {
        QueryBuilder queryBuilder = QueryBuilder.getInstance();
        Query query = queryBuilder.getQuery();
        query.setStart(startTime);
        query.setEnd(endTime);

        List<SubQueries> sqList = new ArrayList<SubQueries>();

        for (String dimension : malarm.getMutipleComputeDimension()) {
            SubQueries sq = new SubQueries();
            sq.setMetric(malarm.getAlarmMetric());
            sq.setAggregator("sum");
            sq.setDownsample("1m-sum");

            HashMap<String, String> newHashMap = Maps.newHashMap();
            newHashMap.put(dimension, dimension);
            //newHashMap.put("ctype", ctype);
            sq.setTags(newHashMap);
            sqList.add(sq);
        }
        query.setQueries(sqList);

        String resContent = null;
        String url = opentsdbDao.getCurrentUrl();
        URI uri = URI.create(url);
        try {
            String json = queryBuilder.build();
            if (logger.isDebugEnabled()) {
                StringBuffer buffer = new StringBuffer();
                buffer.append("[").append(simpleDateFormat.format(new Date(startTime * 1000))).append(" ,")
                        .append(simpleDateFormat.format(new Date(endTime * 1000))).append("]").append(" ,expr :")
                        .append(malarm.getComputeDimension().getComputeField());
                logger.debug("token jobid {} , {}", malarm.getmAlarmId(), buffer.toString());
            }

            resContent = ExecutorApiClient.getInstance().httpPost(uri, null, json);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            int count = 0;
            boolean shouldStop = false;

            while (!shouldStop) {
                try {
                    resContent = ExecutorApiClient.getInstance().httpGet(uri, null);
                    shouldStop = true;
                } catch (IOException e1) {
                    logger.error(e1.getMessage(), e1);
                }

                if (++count > 2 && !shouldStop) {
                    shouldStop = true;
                    opentsdbDao.shuffleClient(true);
                }
            }
        }

        return resContent;
    }

    //TODO 获取opentsdb数据方式
    public Map<String, String> getOpentsData(boolean isContinuous, MAlarm malarm, List<MAlarmCondition> malarmConditions,
                                             long start, long end, ComputeDimension computeDimension) throws IOException {
        String metric = malarm.getAlarmMetric();
        Map<String, String> resContent = null;

        StringBuffer apiBuffer = new StringBuffer();
        // http://10.8.96.121:4242/api/query??summary=true&start=1h-ago&m=sum:ISHOPPING.65{table=ISHOPPING.65_table_shop_custom_module,table=ISHOPPING.65_table_purchase_style_detail}
        apiBuffer.append("&").append("start=").append(start).append("&").append("end=").append(end - 1).append("&")
                .append("m=sum:").append(metric).append("%7B");

        if (malarm.isMapCompute()) {
            for (int i = 0; i < malarmConditions.size(); i++) {
                if (i > 0) {
                    apiBuffer.append(",");
                }
                MAlarmCondition malarmCondition = malarmConditions.get(i);

                apiBuffer.append(computeDimension.getComputeField()).append("=").append(metric).append("_")
                        .append(computeDimension.getComputeField()).append("_")
                        .append(malarmCondition.getMapComputeKey());
            }
        } else {
            String computeField = computeDimension.getComputeField();
            if (StrUtil.empty(computeField)) {
                return null;
            }

            String[] splitedFields = computeField.split(CommonConstant._COMMA);
            for (int i = 0; i < splitedFields.length; i++) {
                if (i > 0) {
                    apiBuffer.append(",");
                }
                String tagKV = splitedFields[i];
                if (computeDimension.getComputeType() == AlarmConstants.ALARM_INDEX_MUTIPLE) {
                    for (String mutiDimension : malarm.getMutipleComputeDimension()) {
                        apiBuffer.append(mutiDimension).append("=").append(mutiDimension);
                    }
                } else {
                    apiBuffer.append(tagKV).append("=").append(tagKV);
                }
            }

            if (!StrUtil.empty(computeDimension.getBoltName())) {
                apiBuffer.append(",bolt.name=").append(computeDimension.getBoltName());
            }
        }

        apiBuffer.append("%7D");
        String url = opentsdbDao.getCurrentUrl() + apiBuffer.toString().trim();
        URI uri = URI.create(url);

        if (logger.isDebugEnabled()) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("url ").append(url).append(" ,time span[")
                    .append(simpleDateFormat.format(new Date(start * 1000))).append(" ,")
                    .append(simpleDateFormat.format(new Date(end * 1000))).append("]");

            buffer.append(" isContinuous ").append(isContinuous);
            logger.debug("token jobid {} ,time span {}", malarm.getmAlarmId(), buffer.toString());
        }

        try {
            resContent = ExecutorApiClient.getInstance().httpGet(uri);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            int count = 0;
            boolean shouldStop = false;
            while (!shouldStop) {
                try {
                    resContent = ExecutorApiClient.getInstance().httpGet(uri);
                    shouldStop = true;
                } catch (IOException e1) {
                    logger.error(e1.getMessage(), e1);
                }

                if (++count > 2 && !shouldStop) {
                    shouldStop = true;
                    opentsdbDao.shuffleClient(true);
                }
            }
        }

        return resContent;
    }

    public DataAlarmTool(ShuffledOpentsdbClient opentsdbDao, SimpleDateFormat simpleDateFormat, Logger logger) {
        super();
        this.opentsdbDao = opentsdbDao;
        this.simpleDateFormat = simpleDateFormat;
        this.logger = logger;
    }

    // TODO 监控avg计算，存在异议
    public Map<String, Double> getMonitoredMapValue(MAlarm malarm, String data) {
        Map<String, Double> result = new HashMap<>();
        JSONArray array = JSON.parseArray(data);
        if (array == null || array.size() == 0) {
            return result;
        }

        // TODO 如果存在多个 怎么处理
        for (int i = 0; i < array.size(); i++) {
            JSONObject object = (JSONObject) array.get(i);
            JSONObject tags = object.getJSONObject("tags");// .get(computeField).toString();
            AlarmComputeResult alarmComputeResult = buildAlarmComputeResult(malarm, tags);

            JSONObject dps = object.getJSONObject("dps");
            if (dps.isEmpty()) {
                logger.info("token jobid {} ,dps is null", malarm.getmAlarmId());
            }

            for (String key : dps.keySet()) {
                alarmComputeResult.update(dps.getDouble(key));
            }

            result.put(alarmComputeResult.getFieldValue(), alarmComputeResult.getValue());
        }

        // TODO mutiple compute
        if (malarm.getComputeDimension().getComputeType() == AlarmConstants.ALARM_INDEX_MUTIPLE) {
            double mutilValue = ExprCaculateUtil.getMathValue(result, malarm.getComputeDimension().getComputeField());
            result.put(malarm.getComputeDimension().getComputeField(), mutilValue);
        }

        return result;
    }

    private AlarmComputeResult buildAlarmComputeResult(MAlarm malarm, JSONObject tags) {
        String computeField = "-";
        String fieldValue = "-";
        int computeType = malarm.getComputeDimension().getComputeType();
        if (tags != null && tags.size() > 0) {
            // fieldValue = getFiledValue(computeType, computeField, tags);
            if (malarm.getComputeDimension().getComputeType() == AlarmConstants.ALARM_INDEX_MUTIPLE) {
                for (Entry<String, Object> entry : tags.entrySet()) {
                    if (malarm.getMutipleComputeDimension().contains(entry.getKey())) {
                        computeField = entry.getKey();
                        fieldValue = entry.getValue().toString();
                        break;
                    }
                }
                computeType = DimensionUtils.computeType2Integer(tags.getString("ctype"));
            } else {
                computeField = malarm.getComputeDimension().getComputeField();
                fieldValue = tags.getString(computeField);
            }
        }

        AlarmComputeResult alarmComputeResult = new AlarmComputeResult(computeField, computeType,
                malarm.isMapCompute(), fieldValue);

        return alarmComputeResult;
    }

}
