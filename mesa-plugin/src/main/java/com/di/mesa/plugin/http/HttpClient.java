package com.di.mesa.plugin.http;

import com.di.mesa.plugin.opentsdb.builder.MetricBuilder;
import com.di.mesa.plugin.opentsdb.request.QueryBuilder;
import com.di.mesa.plugin.opentsdb.response.Response;
import com.di.mesa.plugin.opentsdb.response.SimpleHttpResponse;

import java.io.IOException;

public interface HttpClient extends Client {

	public Response pushMetrics(MetricBuilder builder,
								ExpectResponse exceptResponse) throws IOException;

	public SimpleHttpResponse pushQueries(QueryBuilder builder,
										  ExpectResponse exceptResponse) throws IOException;

	public void shutdown();
}