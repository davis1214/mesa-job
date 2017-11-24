package com.di.mesa.metric.model;

import com.di.mesa.metric.common.AlarmConstants;

import java.util.Date;


public class MAlarmConcern {

	private int id;
	private int mAlarmID;

	private String concerntype;
	private String concernName;
	private String alarmInfoTemplate;
	private String isConcerned;
	private Date createTime;
	private Date updateTime;
	private String concernmembers;

	private String alarmClient;

	public String getAlarmClient() {
		return alarmClient;
	}

	public void setAlarmClient(String alarmClient) {
		this.alarmClient = alarmClient;
	}

	public boolean isWeixinAlarm() {
		String[] clients = alarmClient != null ? alarmClient.split(",") : null;
		if (clients != null) {
			for (String client : clients) {
				if (client.equals(AlarmConstants.ALARM_TYPE_WEIXIN)) {
					return true;
				}
			}

		}
		return false;
	}
	
	public boolean isPhoneAlarm() {
		String[] clients = alarmClient != null ? alarmClient.split(",") : null;
		if (clients != null) {
			for (String client : clients) {
				if (client.equals(AlarmConstants.ALARM_TYPE_PHONE)) {
					return true;
				}
			}

		}
		return false;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getmAlarmID() {
		return mAlarmID;
	}

	public void setmAlarmID(int mAlarmID) {
		this.mAlarmID = mAlarmID;
	}

	public String getConcerntype() {
		return concerntype;
	}

	public void setConcerntype(String concerntype) {
		this.concerntype = concerntype;
	}

	public String getConcernName() {
		return concernName;
	}

	public void setConcernName(String concernName) {
		this.concernName = concernName;
	}

	public String getAlarmInfoTemplate() {
		return alarmInfoTemplate;
	}

	public void setAlarmInfoTemplate(String alarmInfoTemplate) {
		this.alarmInfoTemplate = alarmInfoTemplate;
	}

	public String getIsConcerned() {
		return isConcerned;
	}

	public void setIsConcerned(String isConcerned) {
		this.isConcerned = isConcerned;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	public Date getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Date updateTime) {
		this.updateTime = updateTime;
	}

	public String getConcernmembers() {
		return concernmembers;
	}

	public void setConcernmembers(String concernmembers) {
		this.concernmembers = concernmembers;
	}

	@Override
	public String toString() {
		return "MAlarmConcern [id=" + id + ", mAlarmID=" + mAlarmID + ", concerntype=" + concerntype
				+ ", concernName=" + concernName + ", alarmInfoTemplate=" + alarmInfoTemplate + ", isConcerned="
				+ isConcerned + ", createTime=" + createTime + ", updateTime=" + updateTime + ", concernmembers="
				+ concernmembers + "]";
	}

}
