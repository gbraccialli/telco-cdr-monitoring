package com.github.gbraccialli.storm.commom.bolt;

import java.util.Date;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class SysoutBolt implements IRichBolt {

	private static final long serialVersionUID = 1901513019434766894L;
	private String prefix = "";
	private OutputCollector collector;
	
	public SysoutBolt(String prefix) {
		this.prefix = prefix;
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector){
		System.out.println(this.prefix + "SysoutBolt Started" + new Date(System.currentTimeMillis()).toString());
		this.collector = collector;
	}

	public void execute(Tuple tuple){
		StringBuffer output = new StringBuffer(this.prefix + "SysoutBolt - " + new Date(System.currentTimeMillis()).toString() + " - ");
		for (String field : tuple.getFields()){
			output.append(field + "=" + tuple.getValueByField(field).toString() + ", ");
		}
		output.append(" no more fields");
		System.out.println(output);
		collector.ack(tuple);
	}

	public void cleanup() {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
