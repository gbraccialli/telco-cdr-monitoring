package com.github.gbraccialli.telco.cdr.storm.entity;

public class SessionInformation {

	private String cellId;
	private String networkType;
	private long timesamp;
	
	public SessionInformation(String cellId, String networkType, long timestamp) {
		this.cellId = cellId;
		this.networkType = networkType;
		this.timesamp = timestamp;
	}
	public String getCellId() {
		return cellId;
	}
	public void setCellId(String cellId) {
		this.cellId = cellId;
	}
	public String getNetworkType() {
		return networkType;
	}
	public long getTimesamp() {
		return timesamp;
	}
	public void setTimesamp(long timesamp) {
		this.timesamp = timesamp;
	}
	public void setNetworkType(String networkType) {
		this.networkType = networkType;
	}

}
