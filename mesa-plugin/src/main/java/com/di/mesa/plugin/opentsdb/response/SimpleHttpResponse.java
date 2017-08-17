package com.di.mesa.plugin.opentsdb.response;

/**
 * @author davis
 *
 */
public class SimpleHttpResponse {
	private int statusCode;
	private String content;

	public boolean isSuccess() {
		return statusCode == 200 || statusCode == 204;
	}

	public int getStatusCode() {
		return statusCode;
	}

	public void setStatusCode(int statusCode) {
		this.statusCode = statusCode;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	@Override
	public String toString() {
		return "SimpleHttpResponse [statusCode=" + statusCode + ", content=" + content + "]";
	}
	
}
