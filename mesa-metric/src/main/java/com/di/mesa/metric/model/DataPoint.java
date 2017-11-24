package com.di.mesa.metric.model;

import java.util.HashMap;
import java.util.Map;

public class DataPoint {
	private String metric;
	private long timestamp;
	private String value;
	private HashMap<String, String> tags;
	private String tsuid;

	public DataPoint() {
	}

	public DataPoint(final String metric, final long timestamp, final String value, final HashMap<String, String> tags) {
		this.metric = metric;
		this.timestamp = timestamp;
		this.value = value;
		this.tags = tags;
	}

	public DataPoint(final String tsuid, final long timestamp, final String value) {
		this.tsuid = tsuid;
		this.timestamp = timestamp;
		this.value = value;
	}

	@Override
	public String toString() {
		final StringBuilder buf = new StringBuilder();
		buf.append(" metric=").append(this.metric + "\n");
		buf.append(" ts=").append(this.timestamp + "\n");
		buf.append(" value=").append(this.value).append(" \n");
		if (this.tags != null) {
			for (Map.Entry<String, String> entry : this.tags.entrySet()) {
				buf.append(" ").append(entry.getKey()).append("=").append(entry.getValue() + "\n");
			}
		}
		return buf.toString();
	}

	public String getMetric() {
		return metric;
	}

	public void setMetric(String metric) {
		this.metric = metric;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
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
