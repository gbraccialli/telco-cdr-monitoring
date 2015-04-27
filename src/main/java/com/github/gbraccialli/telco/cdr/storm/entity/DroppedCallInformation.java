package com.github.gbraccialli.telco.cdr.storm.entity;

public class DroppedCallInformation {

	
	private String cellId;
	private String dropReason;
	private long timestamp;
	private long duration;
	private String timestampFormatted;
	
	public DroppedCallInformation(String cellId, String dropReason, long timestamp, String timestampFormatted, long duration) {
		this.cellId = cellId;
		this.dropReason = dropReason;
		this.timestamp = timestamp;
		this.timestampFormatted = timestampFormatted;
		this.duration = duration;
	}
	
	public String getCellId() {
		return cellId;
	}
	public void setCellId(String cellId) {
		this.cellId = cellId;
	}
	public String getDropReason() {
		return dropReason;
	}
	public void setDropReason(String dropReason) {
		this.dropReason = dropReason;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
	public String getTimestampFormatted() {
		return timestampFormatted;
	}

	public void setTimestampFormatted(String timestampFormatted) {
		this.timestampFormatted = timestampFormatted;
	}

	public long getDuration() {
		return duration;
	}

	public void setDuration(long duration) {
		this.duration = duration;
	}
	
	

	

}
