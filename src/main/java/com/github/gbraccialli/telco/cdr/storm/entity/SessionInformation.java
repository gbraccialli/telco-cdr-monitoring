package com.github.gbraccialli.telco.cdr.storm.entity;

public class SessionInformation {

	private String cellId;
	private String networkType;
	private long timestamp;
	
	public SessionInformation(String cellId, String networkType, long timestamp) {
		this.cellId = cellId;
		this.networkType = networkType;
		this.timestamp = timestamp;
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
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timesamp) {
		this.timestamp = timesamp;
	}
	public void setNetworkType(String networkType) {
		this.networkType = networkType;
	}

}
