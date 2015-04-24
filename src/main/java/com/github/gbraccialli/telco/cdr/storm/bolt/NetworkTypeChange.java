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

import com.github.gbraccialli.telco.cdr.storm.entity.SessionInformation;
import com.github.randerzander.StormCommon.bolts.RollingCountFromTimestampBolt;
import com.github.randerzander.StormCommon.utils.TupleHelpers;

public class NetworkTypeChange implements IRichBolt {

	private static final Logger LOG = Logger.getLogger(NetworkTypeChange.class);
	private Calendar cal = Calendar.getInstance();
	private String sessionIdField = "";
	private String cellIdField = "";
	private String networkTypeField = "";
	private String timestampField = "";
	private SimpleDateFormat dateFormat = null;
	private long sessionCleanupIntervalInSeconds = 30*60; //30 minutes
	private long maxTimestamp = 0;

	public NetworkTypeChange(String sessionIdField, String cellIdField, String networkTypField, String timestampField, String dateFormat) {
		this.sessionIdField = sessionIdField;  
		this.cellIdField = cellIdField;
		this.networkTypeField = networkTypField; 
		this.timestampField = timestampField;
		this.dateFormat = new SimpleDateFormat(dateFormat);
	}
	
	public NetworkTypeChange withSessionCleanupIntervalInSeconds(long sessionCleanupIntervalInSeconds) {this.sessionCleanupIntervalInSeconds = sessionCleanupIntervalInSeconds; return this;};

	private static final long serialVersionUID = 1901513019434766894L;
	private OutputCollector collector;
	private ConcurrentHashMap<String,SessionInformation> lastInformationPerSession = new ConcurrentHashMap<String,SessionInformation>();

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
			String sessionId = tuple.getStringByField(sessionIdField);
			cal.setTime(dateFormat.parse(tuple.getStringByField(timestampField)));
			long timestamp = cal.getTimeInMillis();
			if (timestamp > maxTimestamp){
				maxTimestamp = timestamp;
			}

			SessionInformation lastCDR = lastInformationPerSession.get(sessionId);
			SessionInformation newCDR = new SessionInformation(tuple.getStringByField(cellIdField),tuple.getStringByField(networkTypeField),timestamp);
			if (lastCDR != null && !lastCDR.getNetworkType().equals(newCDR.getNetworkType())){
				Values outputTuple = new Values();
				outputTuple.add(sessionId);
				outputTuple.add(tuple.getStringByField(timestampField));
				outputTuple.add(lastCDR.getCellId());
				outputTuple.add(newCDR.getCellId());
				outputTuple.add(lastCDR.getNetworkType() + " to " + newCDR.getNetworkType());
				collector.emit(tuple, outputTuple);
			}
			lastInformationPerSession.put(sessionId, newCDR);

			collector.ack(tuple);
		} catch (ParseException e) {
			//ignore tupple
			LOG.warn("NetworkTypeChange: This tuple invalid Date format and will be ignored: " + tuple.toString());
			
		}

	}

	private void cleanupSessions(){
		LOG.warn("NetworkTypeChange: cleaning old sessions info");
		for (Entry<String,SessionInformation> entry: lastInformationPerSession.entrySet()){
			if (entry.getValue().getTimesamp() < maxTimestamp - sessionCleanupIntervalInSeconds * 1000){
				lastInformationPerSession.remove(entry.getKey());
			}
		}
	}

	public void cleanup() {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(sessionIdField, timestampField, cellIdField + "_from", cellIdField + "_to", networkTypeField + "_change"));
	}

	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, sessionCleanupIntervalInSeconds);
		return conf;
	}


}
