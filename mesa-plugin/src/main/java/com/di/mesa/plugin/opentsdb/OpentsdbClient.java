package com.di.mesa.plugin.opentsdb;


import com.di.mesa.plugin.opentsdb.builder.MetricBuilder;
import com.di.mesa.plugin.http.ExpectResponse;
import com.di.mesa.plugin.http.HttpClient;
import com.di.mesa.plugin.http.HttpClientImpl;
import com.di.mesa.plugin.opentsdb.request.Filter;
import com.di.mesa.plugin.opentsdb.request.Query;
import com.di.mesa.plugin.opentsdb.request.QueryBuilder;
import com.di.mesa.plugin.opentsdb.request.SubQueries;
import com.di.mesa.plugin.opentsdb.response.Response;
import com.di.mesa.plugin.opentsdb.response.SimpleHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Opentsdb读写工具类
 * <p>
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
     * <p>
     * Skips any time series with the given tag key, regardless of the value.
     * This can be useful for situations where a metric has inconsistent tag
     * sets. NOTE: The filter value must be null or an empty string
     */
    public static String FILTER_TYPE_NOT_KEY = "not_key";

    private HttpClient httpClient;

    private String opentsdbUrl;

    public OpentsdbClient(String opentsdbUrl) {
        this.opentsdbUrl = opentsdbUrl;
        this.httpClient = new HttpClientImpl(opentsdbUrl);
    }

    public String getOpentsdbUrl() {
        return opentsdbUrl;
    }

    public void setOpentsdbUrl(String opentsdbUrl) {
        this.opentsdbUrl = opentsdbUrl;
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

    public boolean putData(String metric, long timestamp, Long value, Map<String, String> tagMap) throws Exception {
        MetricBuilder builder = MetricBuilder.getInstance();

        builder.addMetric(metric).setDataPoint(timestamp, value).addTags(tagMap);
        try {
            log.debug("write quest Long value：{}", builder.build());
            Response response = httpClient.pushMetrics(builder, ExpectResponse.SUMMARY);
            log.debug("response.statusCode: {}", response.getStatusCode());
            return response.isSuccess();
        } catch (Exception e) {
            log.error("put data to opentsdb error: ", e);
            throw e;
        }
    }

    public boolean putData(MetricBuilder builder) throws Exception {
        if (builder == null)
            return false;
        Response response = httpClient.pushMetrics(builder, ExpectResponse.SUMMARY);
        return response.isSuccess();
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
            log.debug("write quest Double value：{}", builder.build());
            Response response = httpClient.pushMetrics(builder, ExpectResponse.SUMMARY);
            log.debug("response.statusCode: {}", response.getStatusCode());
            log.debug("put data response=" + response);
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
                          String downsample, long startTime, long endTime) throws IOException {

        QueryBuilder queryBuilder = QueryBuilder.getInstance();
        Query query = queryBuilder.getQuery();

        // query.setStart(DateTimeUtil.parse(startTime,
        // "yyyy-MM-dd HH:mm:ss").getTime() / 1000);
        // query.setEnd(DateTimeUtil.parse(endTime,
        // "yyyy-MM-dd HH:mm:ss").getTime() / 1000);
        query.setStart(startTime);
        query.setEnd(endTime);

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
                System.out.println("query response.content：{}" + spHttpResponse.getContent());
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
     *                   OpentsdbClient.AGGREGATOR_SUM //@param downsample 采样的时间粒度, 如:
     *                   1s,2m,1h,1d,2d
     * @param startTime  查询开始时间,时间格式为yyyy-MM-dd HH:mm:ss
     * @param endTime    查询结束时间,时间格式为yyyy-MM-dd HH:mm:ss
     */
    public String getData(String metric, Filter filter, String aggregator, long startTime, long endTime)
            throws IOException {

        QueryBuilder queryBuilder = QueryBuilder.getInstance();
        Query query = queryBuilder.getQuery();

        // query.setStart(DateTimeUtil.parse(startTime,
        // "yyyy-MM-dd HH:mm:ss").getTime() / 1000);
        // query.setEnd(DateTimeUtil.parse(endTime,
        // "yyyy-MM-dd HH:mm:ss").getTime() / 1000);
        query.setStart(startTime);
        query.setEnd(endTime);

        List<SubQueries> sqList = new ArrayList<SubQueries>();
        SubQueries sq = new SubQueries();
        sq.addMetric(metric);
        sq.addAggregator(aggregator);

        // sq.addDownsample("2m" + "-" + aggregator);
        List<Filter> filters = new ArrayList<Filter>();
        filters.add(filter);
        sq.setFilters(filters);

        sq.setDownsample("1m-avg");
        sqList.add(sq);

        query.setQueries(sqList);

        try {
            log.debug("query request：{}", queryBuilder.build()); // 这行起到校验作用
            // System.out.println("query request："+queryBuilder.build());
            SimpleHttpResponse spHttpResponse = httpClient.pushQueries(queryBuilder, ExpectResponse.DETAIL);
            log.debug("response.content: {}", spHttpResponse.getContent());

            if (spHttpResponse.isSuccess()) {
                // System.out.println("query response.content：{}"+spHttpResponse.getContent());
                return spHttpResponse.getContent();
            }
            return null;
        } catch (IOException e) {
            log.error("get data from opentsdb error: ", e);
            throw e;
        }
    }


    public void shutdown() {
        httpClient.shutdown();
    }

}