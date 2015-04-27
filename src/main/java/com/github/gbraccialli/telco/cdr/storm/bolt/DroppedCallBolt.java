package com.github.gbraccialli.telco.cdr.storm.bolt;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.github.gbraccialli.telco.cdr.storm.entity.DroppedCallInformation;
import com.github.gbraccialli.telco.cdr.storm.entity.SessionInformation;
import com.github.randerzander.StormCommon.bolts.RollingCountFromTimestampBolt;
import com.github.randerzander.StormCommon.utils.TupleHelpers;

public class DroppedCallBolt implements IRichBolt {

	private static final long serialVersionUID = 3726629891206146470L;
	
	private static final Logger LOG = Logger.getLogger(DroppedCallBolt.class);
	private Calendar cal = Calendar.getInstance();
	private String simCardIdField = "";
	private String phoneNumberField = "";
	private String dropReasonField = "";
	private String cellIdField = "";
	private String timestampField = "";
	private SimpleDateFormat dateFormat = null;
	private long sessionCleanupIntervalInSeconds = 30*60; //30 minutes
	private long newCallInSeconds = 60; //1 minute 
	private long maxTimestamp = 0;

	public DroppedCallBolt(String simCardIdField, String phoneNumberField, String dropReasonField, String cellIdField, String timestampField, String dateFormat) {
		this.simCardIdField = simCardIdField;  
		this.phoneNumberField = phoneNumberField;
		this.dropReasonField = dropReasonField;
		this.cellIdField = cellIdField;
		this.timestampField = timestampField;
		this.dateFormat = new SimpleDateFormat(dateFormat);
	}
	
	public DroppedCallBolt withSessionCleanupIntervalInSeconds(long sessionCleanupIntervalInSeconds) {this.sessionCleanupIntervalInSeconds = sessionCleanupIntervalInSeconds; return this;};

	public DroppedCallBolt withNewCallInSeconds(long newCallInSeconds) {this.newCallInSeconds = newCallInSeconds; return this;};

	
	private OutputCollector collector;
	private ConcurrentHashMap<String,DroppedCallInformation> lastInformationPerSimPhone= new ConcurrentHashMap<String,DroppedCallInformation>();

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector){
		this.collector = collector;
	}

	public void execute(Tuple tuple){

		if (TupleHelpers.isTickTuple(tuple)) {
			LOG.debug("NetworkTypeChange: Received tick tuple, cleaning old sessions informations");
			cleanupSessions();
		}
		else {
			processAndAck(tuple);
		}
	}

	private void processAndAck(Tuple tuple){


		try {
			String simCardId = tuple.getStringByField(simCardIdField);
			String phoneNumber = tuple.getStringByField(phoneNumberField);
			String keySimCardPhoneNumber = simCardId + "|" + phoneNumber;
			String timestampFormatted = tuple.getStringByField(timestampField);
			cal.setTime(dateFormat.parse(timestampFormatted));
			long timestamp = cal.getTimeInMillis();
			if (timestamp > maxTimestamp){
				maxTimestamp = timestamp;
			}

			DroppedCallInformation lastCDR = lastInformationPerSimPhone.get(keySimCardPhoneNumber);
			DroppedCallInformation newCDR = new DroppedCallInformation(tuple.getStringByField(cellIdField),tuple.getStringByField(dropReasonField),timestamp,timestampFormatted,Long.parseLong(tuple.getStringByField("duration")));
			if (lastCDR != null && 
					timestamp <= (lastCDR.getTimestamp() + lastCDR.getDuration() * 1000 + newCallInSeconds * 1000) &&
					timestamp >= (lastCDR.getTimestamp() + lastCDR.getDuration() * 1000)
				){
				
				Values outputTuple = new Values();
				outputTuple.add(simCardId);
				outputTuple.add(phoneNumber);
				outputTuple.add(timestampFormatted);
				outputTuple.add(lastCDR.getCellId());
				outputTuple.add(lastCDR.getDropReason());
				collector.emit(tuple, outputTuple);
					
			}
			lastInformationPerSimPhone.put(keySimCardPhoneNumber, newCDR);
			collector.ack(tuple);
		} catch (ParseException e) {
			//ignore tupple
			LOG.warn("NetworkTypeChange: This tuple invalid Date format and will be ignored: " + tuple.toString());
			
		}

	}

	private void cleanupSessions(){
		LOG.warn("NetworkTypeChange: cleaning old sessions info");
		for (Entry<String,DroppedCallInformation> entry: lastInformationPerSimPhone.entrySet()){
			if (entry.getValue().getTimestamp() < maxTimestamp - sessionCleanupIntervalInSeconds * 1000){
				lastInformationPerSimPhone.remove(entry.getKey());
			}
		}
	}

	public void cleanup() {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(simCardIdField, phoneNumberField, timestampField, cellIdField, dropReasonField));
	}

	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, sessionCleanupIntervalInSeconds);
		return conf;
	}


}
