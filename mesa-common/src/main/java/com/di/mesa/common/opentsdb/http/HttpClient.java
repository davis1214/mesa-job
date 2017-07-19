package com.di.mesa.common.opentsdb.http;

import com.di.mesa.common.opentsdb.builder.MetricBuilder;
import com.di.mesa.common.opentsdb.request.QueryBuilder;
import com.di.mesa.common.opentsdb.response.Response;
import com.di.mesa.common.opentsdb.response.SimpleHttpResponse;

import java.io.IOException;

public interface HttpClient extends Client {

	public Response pushMetrics(MetricBuilder builder,
								ExpectResponse exceptResponse) throws IOException;

	public SimpleHttpResponse pushQueries(QueryBuilder builder,
										  ExpectResponse exceptResponse) throws IOException;

	public void shutdown();
}