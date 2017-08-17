package com.di.mesa.plugin.http;

import com.di.mesa.plugin.opentsdb.constants.ConnectorParams;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URI;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Client class that will be used to handle all Restful API calls between
 * Executor and the host application.
 */
public class ExecutorApiClient extends RestfulApiClient<String> {
    private static ExecutorApiClient instance = null;
    private ConcurrentHashMap<String, Integer> requestMap = new ConcurrentHashMap<>();

    private HttpRequestGuarder guarder = null;

    private ExecutorApiClient() {
        if (guarder == null || guarder.isInterrupted()) {
            guarder = new HttpRequestGuarder();
            guarder.start();
        }
    }

    /**
     * Singleton method to return the instance of the current object.
     */
    public static ExecutorApiClient getInstance() {
        if (null == instance) {
            instance = new ExecutorApiClient();
        }

        return instance;
    }

    /**
     * Implementing the parseResponse function to return de-serialized Json
     * object.
     *
     * @param response the returned response from the HttpClient.
     * @return de-serialized object from Json or null if the response doesn't
     * have a body.
     */
    @Override
    protected String parseResponse(HttpResponse response) throws HttpResponseException, IOException {
        final StatusLine statusLine = response.getStatusLine();
        String responseBody = response.getEntity() != null ? EntityUtils.toString(response.getEntity()) : "";

        if (statusLine.getStatusCode() >= 300) {
            logger.error(String.format("unable to parse response as the response status is %s",
                    statusLine.getStatusCode()));
            throw new HttpResponseException(statusLine.getStatusCode(), responseBody);
        }

        return responseBody;
    }

    /**
     * function to dispatch the request and pass back the response.
     */
    protected String sendAndReturn(final HttpUriRequest request, String mode) throws IOException {
        if (mode.equals(ConnectorParams.SYNCHRONOUS_MODE_SYNC)) {
            CloseableHttpClient client = HttpClients.createDefault();
            try {
                return this.parseResponse(client.execute(request));
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            client.close();
        } else {
            CloseableHttpAsyncClient httpclient = getHttpAsyncClient();
            httpclient.execute(request, new FutureCallback<HttpResponse>() {
                public void failed(final Exception ex) {
                    logger.error(ex.getMessage());

                    String url = request.getURI().toString();
                    Integer count = requestMap.get(url);
                    if (count != null && count > 0) {

                        if (count > 5) {
                            requestMap.remove(url);
                        } else {
                            requestMap.put(url, requestMap.get(url) + 1);
                        }
                    } else {
                        requestMap.put(url, 1);
                    }
                }

                public void cancelled() {
                }

                @Override
                public void completed(HttpResponse result) {
                    logger.info(result.getStatusLine().toString());
                    try {
                        String parsedResult = parseResponse(result);
                        logger.info(parsedResult);
                    } catch (IOException e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            });

            return "{\"result\":\"success\",\"desc\":\"ascyn http request successed\"}";
        }

        return "{\"result\":\"success\",\"desc\":\"http request successed\"}";
    }

    class HttpRequestGuarder extends Thread {
        public HttpRequestGuarder() {

        }

        AtomicBoolean shouldStop = new AtomicBoolean(false);

        @Override
        public void run() {
            while (!shouldStop.get()) {
                try {
                    for (Entry<String, Integer> entry : requestMap.entrySet()) {
                        ExecutorApiClient.getInstance().httpGet(URI.create(entry.getKey()), null,
                                ConnectorParams.SYNCHRONOUS_MODE_ASYNC);
                    }
                } catch (IOException e) {
                }

                sleep();
            }
        }

        private void sleep() {
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }

        public void interupt() {
            shouldStop.set(true);
        }

    }

}
