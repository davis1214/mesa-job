package com.di.mesa.metric.client;

import com.di.mesa.metric.client.request.QueryBuilder;
import com.di.mesa.metric.client.response.SimpleHttpResponse;
import com.di.mesa.metric.client.builder.MetricBuilder;
import com.di.mesa.metric.client.response.Response;

import java.io.IOException;

public interface HttpClient extends Client {

	public Response pushMetrics(MetricBuilder builder,
								ExpectResponse exceptResponse) throws IOException;

	public SimpleHttpResponse pushQueries(QueryBuilder builder,
										  ExpectResponse exceptResponse) throws IOException;

    public void shutdown();
}