package com.di.mesa.metric.model;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ComputeDimension {

    private String computeName;
    private String computeField;
    private int computeType;
    private int timeGap;
    private String boltName;

    @Override
    public String toString() {
        return "ComputeDimension{" +
                "computeName='" + computeName + '\'' +
                ", computeField='" + computeField + '\'' +
                ", computeType=" + computeType +
                ", timeGap=" + timeGap +
                ", boltName='" + boltName + '\'' +
                '}';
    }

    public String getBoltName() {
        return boltName;
    }

    public void setBoltName(String boltName) {
        this.boltName = boltName;
    }

    public String getComputeName() {
        return computeName;
    }

    public void setComputeName(String computeName) {
        this.computeName = computeName;
    }

    public String getComputeField() {
        return computeField;
    }

    public void setComputeField(String computeField) {
        this.computeField = computeField;
    }

    public int getComputeType() {
        return computeType;
    }

    public void setComputeType(int computeType) {
        this.computeType = computeType;
    }

    public int getTimeGap() {
        return timeGap;
    }

    public void setTimeGap(int timeGap) {
        this.timeGap = timeGap;
    }

    public boolean isMapCompute() {
        return this.getComputeType() >= 7 && this.getComputeType() <= 12;
    }

}
