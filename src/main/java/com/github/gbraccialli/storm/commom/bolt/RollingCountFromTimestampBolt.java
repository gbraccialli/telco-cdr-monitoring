package com.github.gbraccialli.storm.commom.bolt;

import java.text.ParseException;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import com.github.gbraccialli.storm.commom.utils.TupleHelpers;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class RollingCountFromTimestampBolt implements IRichBolt {

	private static final long serialVersionUID = -1859197677438265391L;
	private static final Logger LOG = Logger.getLogger(RollingCountFromTimestampBolt.class);


	private OutputCollector collector;
	private Calendar cal = Calendar.getInstance();

	private SimpleDateFormat dateFormat = null; 
	private String keyFieldsDelimiter = "|";
	private Fields keyFields = null;
	private String timestampField = "";
	private int numberOfSecondsPerChunk = 1*60; //1 minute
	private int numberOfWindowChunks = 30;
	private int delayInSeconds = 0;
	private long maxTimestampRounded;

	private Map<String,Map<Long,Long>> hashmapKeysTimestampsCounts = new ConcurrentHashMap<String,Map<Long,Long>>();

	public RollingCountFromTimestampBolt withKeyFields(String[] idFields){ this.keyFields = new Fields(idFields); return this; }
	public RollingCountFromTimestampBolt withKeyFieldsDelimiter(String idFieldsDelimiter){ this.keyFieldsDelimiter = idFieldsDelimiter; return this; }
	public RollingCountFromTimestampBolt withNumberOfSecondsPerChunk(int numberOfSecondsPerChunk){ this.numberOfSecondsPerChunk = numberOfSecondsPerChunk; return this; }
	public RollingCountFromTimestampBolt withNumberOfWindowChunks(int numberOfWindowChunks){ this.numberOfWindowChunks = numberOfWindowChunks; return this; }
	public RollingCountFromTimestampBolt withDelayInSeconds(int delayInSeconds){ this.delayInSeconds = delayInSeconds; return this; }
	public RollingCountFromTimestampBolt withTimestampField(String timestampField, String dateFormat){ 
		this.timestampField = timestampField;
		this.dateFormat = new SimpleDateFormat(dateFormat);
		return this;
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector){
		this.collector = collector;
		cal = Calendar.getInstance();
	}

	public void execute(Tuple tuple) {
		if (TupleHelpers.isTickTuple(tuple)) {
			LOG.debug("RollingCountFromTimestampBolt: Received tick tuple, triggering emit of current window counts");
			emitCurrentWindowCounts();
		}
		else {
			countAndAck(tuple);
		}
	}

	private void emitCurrentWindowCounts() {

		for (Entry<String,Map<Long,Long>> entry : hashmapKeysTimestampsCounts.entrySet()){
			Map<Long, Long> hashmapTimestampsCounts = entry.getValue();
			String key = entry.getKey();
			long windowCount = 0;
			for (Long timestampRounded: hashmapTimestampsCounts.keySet()){
				if (timestampRounded < maxTimestampRounded - delayInSeconds*1000 - numberOfWindowChunks * numberOfSecondsPerChunk * 1000){
					hashmapTimestampsCounts.remove(timestampRounded);
				}else{
					if (timestampRounded <= maxTimestampRounded - delayInSeconds*1000){
						windowCount += hashmapTimestampsCounts.get(timestampRounded);
					}
					//else wait, emit counts only after delay period
				}
			}
			if (hashmapTimestampsCounts.size() == 0){ 
				hashmapKeysTimestampsCounts.remove(key);
			}
			if (windowCount > 0){
				cal.setTimeInMillis(maxTimestampRounded - delayInSeconds*1000);
				Values values = new Values();
				values.addAll(Arrays.asList(key.split(Pattern.quote(keyFieldsDelimiter))));
				values.add(dateFormat.format(cal.getTime()));
				values.add(key);
				values.add(windowCount);
				collector.emit(values);
			}
		}
	}

	private void countAndAck(Tuple tuple) {

		try{
			cal.setTime(dateFormat.parse(tuple.getStringByField(timestampField)));
			long timestampRounded = cal.getTimeInMillis() / (numberOfSecondsPerChunk * 1000) * (numberOfSecondsPerChunk * 1000);

			if (timestampRounded > maxTimestampRounded){
				maxTimestampRounded = timestampRounded;
			}
			if (timestampRounded < maxTimestampRounded - delayInSeconds*1000 - numberOfWindowChunks * numberOfSecondsPerChunk * 1000){
				LOG.warn("RollingCountFromTimestampBolt: This tuple is too old to the current window count and will be ignored, please increase number of Window Chunks:" + tuple.toString());
			}else{
				StringBuffer sbKeys = new StringBuffer();
				for (String field : keyFields){
					sbKeys.append(tuple.getStringByField(field) + keyFieldsDelimiter);
				}
				String keys = sbKeys.toString();
				Map<Long,Long> hashmapTimestampsCounts = hashmapKeysTimestampsCounts.get(keys);
				Long count;
				if (hashmapTimestampsCounts == null){
					hashmapTimestampsCounts = new ConcurrentHashMap<Long,Long>();
					count = new Long(1);
				}else{
					count = hashmapTimestampsCounts.get(timestampRounded);
					if (count == null){
						count = new Long(1);
					}else{
						count = new Long(count.longValue()+1);
					}
				}
				hashmapTimestampsCounts.put(timestampRounded, count);
				hashmapKeysTimestampsCounts.put(keys, hashmapTimestampsCounts);
				
			}
		}catch (ParseException e){
			//ignore tupple
			LOG.warn("RollingCountFromTimestampBolt: This tuple invalid Date format and will be ignored: " + tuple.toString());
		}
		collector.ack(tuple);
	}

	public void cleanup() {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		ArrayList<String> fields = new ArrayList<String>(keyFields.toList());
		fields.add(timestampField + "_rounded");
		fields.add("key");
		fields.add("count");
		declarer.declare(new Fields(fields));
	}

	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, numberOfSecondsPerChunk);
		return conf;
	}


}

