package com.di.mesa.metric.client;

import com.di.mesa.metric.client.builder.MetricBuilder;
import com.di.mesa.metric.client.request.QueryBuilder;
import com.di.mesa.metric.client.response.Response;
import com.di.mesa.metric.client.response.SimpleHttpResponse;

import java.io.IOException;

public interface Client {

	String PUT_POST_API = "/api/put";

    String QUERY_POST_API = "/api/query";

	/**
	 * Sends metrics from the builder to the KairosDB server.
	 *
	 * @param builder
	 *            metrics builder
	 * @return response from the server
	 * @throws IOException
	 *             problem occurred sending to the server
	 */
	Response pushMetrics(MetricBuilder builder) throws IOException;

	SimpleHttpResponse pushQueries(QueryBuilder builder) throws IOException;


}