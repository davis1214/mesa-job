package com.di.mesa.metric.model;

import java.util.HashMap;

public class RequestContent {
	private String metric;
	private long startEpoch;
	private long endEpoch;
	private HashMap<String, String> tags;
	private String tsuid;

	public RequestContent() {
	}

	public RequestContent(String metric, long startEpoch, long endEpoch, HashMap<String, String> tags) {
		super();
		this.metric = metric;
		this.startEpoch = startEpoch;
		this.endEpoch = endEpoch;
		this.tags = tags;
	}

	@Override
	public String toString() {
		return "RequestContent [metric=" + metric + ", startEpoch=" + startEpoch + ", endEpoch=" + endEpoch + ", tags="
				+ tags + ", tsuid=" + tsuid + "]";
	}

	public String getMetric() {
		return metric;
	}

	public void setMetric(String metric) {
		this.metric = metric;
	}

	public long getStartEpoch() {
		return startEpoch;
	}

	public void setStartEpoch(long startEpoch) {
		this.startEpoch = startEpoch;
	}

	public long getEndEpoch() {
		return endEpoch;
	}

	public void setEndEpoch(long endEpoch) {
		this.endEpoch = endEpoch;
	}

	public HashMap<String, String> getTags() {
		return tags;
	}

	public void setTags(HashMap<String, String> tags) {
		this.tags = tags;
	}

	public String getTsuid() {
		return tsuid;
	}

	public void setTsuid(String tsuid) {
		this.tsuid = tsuid;
	}

}
