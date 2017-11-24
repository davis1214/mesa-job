package com.di.mesa.metric.job;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.di.mesa.metric.util.StrUtil;
import com.di.mesa.metric.client.ExpectResponse;
import com.di.mesa.metric.client.HttpClient;
import com.di.mesa.metric.client.HttpClientImpl;
import com.di.mesa.metric.client.builder.MetricBuilder;
import com.di.mesa.metric.client.request.Filter;
import com.di.mesa.metric.client.request.Query;
import com.di.mesa.metric.client.request.QueryBuilder;
import com.di.mesa.metric.client.request.SubQueries;
import com.di.mesa.metric.client.response.Response;
import com.di.mesa.metric.client.response.SimpleHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Opentsdb读写工具类
 * <p/>
 */
public class OpentsdbClient {

    private static Logger log = LoggerFactory.getLogger(OpentsdbClient.class);

    /**
     * tagv的过滤规则: 精确匹配多项迭代值，多项迭代值以'|'分隔，大小写敏感
     */
    public static String FILTER_TYPE_LITERAL_OR = "literal_or";

    /**
     * tagv的过滤规则: 通配符匹配，大小写敏感
     */
    public static String FILTER_TYPE_WILDCARD = "wildcard";

    /**
     * tagv的过滤规则: 正则表达式匹配
     */
    public static String FILTER_TYPE_REGEXP = "regexp";

    /**
     * tagv的过滤规则: 精确匹配多项迭代值，多项迭代值以'|'分隔，忽略大小写
     */
    public static String FILTER_TYPE_ILITERAL_OR = "iliteral_or";

    /**
     * tagv的过滤规则: 通配符匹配，忽略大小写
     */
    public static String FILTER_TYPE_IWILDCARD = "iwildcard";

    /**
     * tagv的过滤规则: 通配符取非匹配，大小写敏感
     */
    public static String FILTER_TYPE_NOT_LITERAL_OR = "not_literal_or";

    /**
     * tagv的过滤规则: 通配符取非匹配，忽略大小写
     */
    public static String FILTER_TYPE_NOT_ILITERAL_OR = "not_iliteral_or";

    /**
     * tagv的过滤规则:
     * <p/>
     * Skips any time series with the given tag key, regardless of the value.
     * This can be useful for situations where a metric has inconsistent tag
     * sets. NOTE: The filter value must be null or an empty string
     */
    public static String FILTER_TYPE_NOT_KEY = "not_key";

    private HttpClient httpClient;

    private String opentsdbUrl;

    public OpentsdbClient() {
        this.httpClient = new HttpClientImpl("http://10.8.96.120:4242/api/query/?summary=true");
    }

    public OpentsdbClient(String opentsdbUrl) {
        this.opentsdbUrl = opentsdbUrl;
        this.httpClient = new HttpClientImpl(opentsdbUrl);
    }

    /**
     * 写入数据
     *
     * @param metric    指标
     * @param timestamp 时间点
     * @param value
     * @param tagMap
     * @return
     * @throws Exception
     */
    public boolean putData(String metric, Date timestamp, Long value, Map<String, String> tagMap) throws Exception {
        long timsSecs = timestamp.getTime() / 1000;
        return this.putData(metric, timsSecs, value, tagMap);
    }

    /**
     * 写入数据
     *
     * @param metric    指标
     * @param timestamp 时间点
     * @param value
     * @param tagMap
     * @return
     * @throws Exception
     */
    public boolean putData(String metric, Date timestamp, Double value, Map<String, String> tagMap) throws Exception {
        long timsSecs = timestamp.getTime() / 1000;
        return this.putData(metric, timsSecs, value, tagMap);
    }

    /**
     * 写入数据
     *
     * @param metric    指标
     * @param timestamp 转化为秒的时间点
     * @param value
     * @param tagMap
     * @return
     * @throws Exception
     */
    public boolean putData(String metric, long timestamp, Long value, Map<String, String> tagMap) throws Exception {
        MetricBuilder builder = MetricBuilder.getInstance();
        builder.addMetric(metric).setDataPoint(timestamp, value).addTags(tagMap);
        try {
            log.debug("write quest：{}", builder.build());
            System.out.println("write quest：" + builder.build());
            Response response = httpClient.pushMetrics(builder, ExpectResponse.SUMMARY);
            log.debug("response.statusCode: {}", response.getStatusCode());
            System.out.println("response.statusCode:" + response.getStatusCode());
            return response.isSuccess();
        } catch (Exception e) {
            e.printStackTrace();
            log.error("put data to opentsdb error: ", e);
            throw e;
        }
    }

    /**
     * 写入数据
     *
     * @param metric    指标
     * @param timestamp 转化为秒的时间点
     * @param value
     * @param tagMap
     * @return
     * @throws Exception
     */
    public boolean putData(String metric, long timestamp, Double value, Map<String, String> tagMap) throws Exception {
        MetricBuilder builder = MetricBuilder.getInstance();
        builder.addMetric(metric).setDataPoint(timestamp, value).addTags(tagMap);
        try {
            log.debug("write quest：{}", builder.build());
            Response response = httpClient.pushMetrics(builder, ExpectResponse.SUMMARY);
            log.debug("response.statusCode: {}", response.getStatusCode());
            return response.isSuccess();
        } catch (Exception e) {
            log.error("put data to opentsdb error: ", e);
            throw e;
        }
    }

    /**
     * 查询数据，返回的数据为json格式，结构为： "[ " { " metric: mysql.innodb.row_lock_time, "
     * tags: { " host: web01, " dc: beijing " }, " aggregateTags: [], " dps: { "
     * 1435716527: 1234, " 1435716529: 2345 " } " }, " { " metric:
     * mysql.innodb.row_lock_time, " tags: { " host: web02, " dc: beijing " }, "
     * aggregateTags: [], " dps: { " 1435716627: 3456 " } " } "]";
     *
     * @param metric     要查询的指标
     * @param tagk       tagk
     * @param tagvFtype  tagv的过滤规则
     * @param tagvFilter tagv的匹配字符
     * @param aggregator 查询的聚合类型, 如: OpentsdbClient.AGGREGATOR_AVG,
     *                   OpentsdbClient.AGGREGATOR_SUM
     * @param downsample 采样的时间粒度, 如: 1s,2m,1h,1d,2d
     * @param startTime  查询开始时间,时间格式为yyyy-MM-dd HH:mm:ss
     * @param endTime    查询结束时间,时间格式为yyyy-MM-dd HH:mm:ss
     * @return
     */
    public String getData(String metric, String tagk, String tagvFtype, String tagvFilter, String aggregator,
                          String downsample, String startTime, String endTime) throws IOException {

        QueryBuilder queryBuilder = QueryBuilder.getInstance();
        Query query = queryBuilder.getQuery();
        query.setStart(StrUtil.string2Date("yyyy-MM-dd HH:mm:ss", startTime).getTime() / 1000);
        query.setEnd(StrUtil.string2Date("yyyy-MM-dd HH:mm:ss", endTime).getTime() / 1000);

        List<SubQueries> sqList = new ArrayList<SubQueries>();
        SubQueries sq = new SubQueries();
        sq.setMetric(metric);
        sq.setAggregator(aggregator);

        List<Filter> filters = new ArrayList<Filter>();
        Filter filter = new Filter();
        filter.setTagk(tagk);
        filter.setType(tagvFtype);
        filter.setFilter(tagvFilter);
        filter.setGroupBy(Boolean.TRUE);
        filters.add(filter);

        sq.setFilters(filters);
        sq.setDownsample(downsample + "-" + aggregator);
        sqList.add(sq);
        query.setQueries(sqList);

        try {
            log.debug("query request：{}", queryBuilder.build()); // 这行起到校验作用
            SimpleHttpResponse spHttpResponse = httpClient.pushQueries(queryBuilder, ExpectResponse.DETAIL);
            log.debug("response.content: {}", spHttpResponse.getContent());

            if (spHttpResponse.isSuccess()) {
                return spHttpResponse.getContent();
            }
            return null;
        } catch (IOException e) {
            log.error("get data from opentsdb error: ", e);
            throw e;
        }
    }

    /**
     * 查询数据，返回的数据为json格式。
     *
     * @param metric     要查询的指标
     * @param filter     查询过滤的条件, 原来使用的tags在v2.2后已不适用 filter.setType(): 设置过滤类型, 如:
     *                   wildcard, regexp filter.setTagk(): 设置tag filter.setFilter():
     *                   根据type设置tagv的过滤表达式, 如: hqdApp|hqdWechat
     *                   filter.setGroupBy():设置成true, 不设置或设置成false会导致读超时
     * @param aggregator 查询的聚合类型, 如: OpentsdbClient.AGGREGATOR_AVG,
     *                   OpentsdbClient.AGGREGATOR_SUM
     * @param downsample 采样的时间粒度, 如: 1s,2m,1h,1d,2d
     * @param startTime  查询开始时间,时间格式为yyyy-MM-dd HH:mm:ss
     * @param endTime    查询结束时间,时间格式为yyyy-MM-dd HH:mm:ss
     */
    public String getData(String metric, Filter filter, String aggregator, String downsample, String startTime,
                          String endTime) throws IOException {

        QueryBuilder queryBuilder = QueryBuilder.getInstance();
        Query query = queryBuilder.getQuery();

        query.setStart(StrUtil.string2Date("yyyy-MM-dd HH:mm:ss", startTime).getTime() / 1000);
        query.setEnd(StrUtil.string2Date("yyyy-MM-dd HH:mm:ss", endTime).getTime() / 1000);

        List<SubQueries> sqList = new ArrayList<SubQueries>();
        SubQueries sq = new SubQueries();
        sq.addMetric(metric);
        sq.addAggregator(aggregator);

        List<Filter> filters = new ArrayList<Filter>();
        filters.add(filter);
        sq.setFilters(filters);

        sq.setDownsample(downsample + "-" + aggregator);
        sqList.add(sq);

        query.setQueries(sqList);

        try {
            log.debug("query request：{}", queryBuilder.build()); // 这行起到校验作用
            SimpleHttpResponse spHttpResponse = httpClient.pushQueries(queryBuilder, ExpectResponse.DETAIL);
            log.debug("response.content: {}", spHttpResponse.getContent());

            if (spHttpResponse.isSuccess()) {
                return spHttpResponse.getContent();
            }
            return null;
        } catch (IOException e) {
            log.error("get data from opentsdb error: ", e);
            throw e;
        }
    }

    public String getData(QueryBuilder queryBuilder) throws IOException {
        try {
            SimpleHttpResponse spHttpResponse = httpClient.pushQueries(queryBuilder, ExpectResponse.DETAIL);
            System.out.println(spHttpResponse.getContent());

            if (spHttpResponse.isSuccess()) {
                return spHttpResponse.getContent();
            }
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            log.error("get data from opentsdb error: ", e);
            throw e;
        }
    }

    public QueryBuilder getQuery(String metric, List<Filter> filters, String aggregator, String downsample)
            throws IOException {

        QueryBuilder queryBuilder = QueryBuilder.getInstance();
        Query query = queryBuilder.getQuery();

        List<SubQueries> sqList = new ArrayList<SubQueries>();
        SubQueries sq = new SubQueries();
        sq.addMetric(metric);
        sq.addAggregator(aggregator);

        // Map<String,String> tags = new HashMap<>();
        // tags.put("level", "level");
        // sq.setTags(tags);
        sq.setFilters(filters);
        sq.setDownsample(downsample + "-" + aggregator);
        sqList.add(sq);

        query.setQueries(sqList);
        return queryBuilder;
    }

    public String getData(String metric, List<Filter> filters, String aggregator, String downsample, long startTime,
                          long endTime) throws IOException {
        QueryBuilder queryBuilder = QueryBuilder.getInstance();
        Query query = queryBuilder.getQuery();
        query.setStart(startTime);
        query.setEnd(endTime);

        List<SubQueries> sqList = new ArrayList<SubQueries>();
        SubQueries sq = new SubQueries();
        sq.addMetric(metric);
        sq.addAggregator(aggregator);
        sq.setFilters(filters);
        sq.setDownsample(downsample + "-" + aggregator);
        sqList.add(sq);
        query.setQueries(sqList);
        try {
            log.debug("query request：{}", queryBuilder.build()); // 这行起到校验作用
            SimpleHttpResponse spHttpResponse = httpClient.pushQueries(queryBuilder, ExpectResponse.DETAIL);
            log.debug("response.content: {}", spHttpResponse.getContent());
            if (spHttpResponse.isSuccess()) {
                return spHttpResponse.getContent();
            }
            return null;
        } catch (IOException e) {
            log.error("get data from opentsdb error: ", e);
            throw e;
        }
    }

    /**
     * 查询数据，返回tags与时序值的映射: Map<tags, Map<时间点, value>>
     *
     * @param metric     要查询的指标
     * @param tagk       tagk
     * @param tagvFtype  tagv的过滤规则
     * @param tagvFilter tagv的匹配字符
     * @param aggregator 查询的聚合类型, 如: OpentsdbClient.AGGREGATOR_AVG,
     *                   OpentsdbClient.AGGREGATOR_SUM
     * @param downsample 采样的时间粒度, 如: 1s,2m,1h,1d,2d
     * @param startTime  查询开始时间,时间格式为yyyy-MM-dd HH:mm:ss
     * @param endTime    查询结束时间,时间格式为yyyy-MM-dd HH:mm:ss
     * @param retTimeFmt 返回的结果集中，时间点的格式, 如：yyyy-MM-dd HH:mm:ss 或 yyyyMMddHH 等
     * @return Map<tags, Map<时间点, value>>
     * @throws IOException
     */
    public Map<String, Map<String, Object>> getData(String metric, String tagk, String tagvFtype, String tagvFilter,
                                                    String aggregator, String downsample, String startTime, String endTime, String retTimeFmt)
            throws IOException {
        String resContent = this.getData(metric, tagk, tagvFtype, tagvFilter, aggregator, downsample, startTime,
                endTime);
        return this.convertContentToMap(resContent, retTimeFmt);
    }

    /**
     * 查询数据，返回tags与时序值的映射: Map<tags, Map<时间点, value>>
     *
     * @param metric     要查询的指标
     * @param filter     查询过滤的条件, 原来使用的tags在v2.2后已不适用 filter.setType(): 设置过滤类型, 如:
     *                   wildcard, regexp filter.setTagk(): 设置tag filter.setFilter():
     *                   根据type设置tagv的过滤表达式, 如: hqdApp|hqdWechat
     *                   filter.setGroupBy():设置成true, 不设置或设置成false会导致读超时
     * @param aggregator 查询的聚合类型, 如: OpentsdbClient.AGGREGATOR_AVG,
     *                   OpentsdbClient.AGGREGATOR_SUM
     * @param downsample 采样的时间粒度, 如: 1s,2m,1h,1d,2d
     * @param startTime  查询开始时间, 时间格式为yyyy-MM-dd HH:mm:ss
     * @param endTime    查询结束时间, 时间格式为yyyy-MM-dd HH:mm:ss
     * @param retTimeFmt 返回的结果集中，时间点的格式, 如：yyyy-MM-dd HH:mm:ss 或 yyyyMMddHH 等
     * @return Map<tags, Map<时间点, value>>
     */
    public Map<String, Map<String, Object>> getData(String metric, Filter filter, String aggregator, String downsample,
                                                    String startTime, String endTime, String retTimeFmt) throws IOException {
        String resContent = this.getData(metric, filter, aggregator, downsample, startTime, endTime);
        return this.convertContentToMap(resContent, retTimeFmt);
    }

    public Map<String, Map<String, Object>> convertContentToMap(String resContent, String retTimeFmt) {

        // Map<tags, Map<时间点, value>>
        Map<String, Map<String, Object>> tagsValuesMap = new HashMap<String, Map<String, Object>>();

        if (resContent == null || "".equals(resContent.trim())) {
            return tagsValuesMap;
        }

        JSONArray array = (JSONArray) JSONObject.parse(resContent);
        if (array != null) {
            for (int i = 0; i < array.size(); i++) {
                JSONObject obj = (JSONObject) array.get(i);
                JSONObject tags = (JSONObject) obj.get("tags");
                JSONObject dps = (JSONObject) obj.get("dps");

                // timeValueMap.putAll(dps);
                Map<String, Object> timeValueMap = new HashMap<String, Object>();
                for (Iterator<String> it = dps.keySet().iterator(); it.hasNext(); ) {
                    String timstamp = it.next();
                    Date datetime = new Date(Long.parseLong(timstamp) * 1000);
                    timeValueMap.put(StrUtil.date2String(retTimeFmt, datetime), dps.get(timstamp));
                }
                tagsValuesMap.put(tags.toString(), timeValueMap);
            }
        }
        return tagsValuesMap;
    }

    public String getOpentsdbUrl() {
        return opentsdbUrl;
    }

    public void setOpentsdbUrl(String opentsdbUrl) {
        this.opentsdbUrl = opentsdbUrl;
    }

}
