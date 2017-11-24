package com.di.mesa.metric.alarms;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.di.mesa.metric.common.ComupteTypeConstant;

public class AlarmComputeResult {
	private long count;
	private int computeType;
	private double computeValue;

	private boolean isMapCompute;
	private String fieldValue;

	private HyperLogLogPlus card;

	public AlarmComputeResult(String computeField, int computeType, boolean isMapCompute, String fieldValue) {
		this.computeType = computeType;
		this.isMapCompute = isMapCompute;
		if (isMapCompute) {
			this.fieldValue = fieldValue.substring(fieldValue.lastIndexOf(computeField) + computeField.length() + 1);
		} else {
			this.fieldValue = fieldValue;
		}
		
		switch (computeType) {
		case ComupteTypeConstant.MAX:
			this.computeValue = Integer.MIN_VALUE;
			break;
		case ComupteTypeConstant.MIN:
			this.computeValue = Integer.MAX_VALUE;
			break;
		case ComupteTypeConstant.UV:
			card = new HyperLogLogPlus(14, 25);
			break;
		}
	}

	public double getValue() {
		if (computeType == ComupteTypeConstant.UV) {
			return this.card.cardinality();
		}

		if (computeType == ComupteTypeConstant.AVG || computeType == ComupteTypeConstant.MAP_AVG) {
			try {
				return this.computeValue / this.count;
			} catch (Exception e) {
				return 0f;
			}
		}
		return this.computeValue;
	}

	public void update(double statValue) {
		count += 1;

		switch (computeType) {
		case ComupteTypeConstant.PV:
			this.computeValue += statValue;
			break;
		case ComupteTypeConstant.AVG:
			this.computeValue += statValue;
			break;
		case ComupteTypeConstant.SUM:
			this.computeValue += statValue;
			break;
		case ComupteTypeConstant.MAX:
			this.computeValue = Math.max(computeValue, statValue);
			break;
		case ComupteTypeConstant.MIN:
			this.computeValue = Math.min(computeValue, statValue);
			break;
		case ComupteTypeConstant.UV:
			this.card.offer(statValue);
			break;
		case ComupteTypeConstant.MAP_PV:
			this.computeValue += statValue;
			break;
		case ComupteTypeConstant.MAP_AVG:
			this.computeValue += statValue;
			break;
		case ComupteTypeConstant.MAP_SUM:
			this.computeValue += statValue;
			break;
		case ComupteTypeConstant.MAP_MIN:
			this.computeValue = Math.min(computeValue, statValue);
			break;
		case ComupteTypeConstant.MAP_MAX:
			this.computeValue = Math.max(computeValue, statValue);
			break;
		}
	}

	public String getFieldValue() {
		return fieldValue;
	}

}
