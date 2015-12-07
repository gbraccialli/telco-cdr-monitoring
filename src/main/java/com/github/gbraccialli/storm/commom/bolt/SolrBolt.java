package com.github.gbraccialli.storm.commom.bolt;

import java.text.ParseException;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class SolrBolt implements IRichBolt {

	private static final long serialVersionUID = -1859197677438265391L;
	private OutputCollector collector;
	private HttpSolrServer solrServer = null;
	private String solrServerUrl = "";
	private Calendar cal = null;


	private String fixedValueFieldName = "";
	private String fixedValueFieldValue = "";
	private SimpleDateFormat dateFormat = null; 
	private SimpleDateFormat solrDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	private String idFieldsDelimiter = "|";
	
	private Fields fieldsSubset = null;
	private Fields idFields = null;
	private Fields dateConversionFields = null;
	
    public SolrBolt withSolrServer(String solrServerUrl){ this.solrServerUrl = solrServerUrl; return this; }
    public SolrBolt withFixedValueField(String fixedValueFieldName, String fixedValueFieldValue){ this.fixedValueFieldName = fixedValueFieldName; this.fixedValueFieldValue = fixedValueFieldValue; return this; }
    
    public SolrBolt withFieldsSubset(String[] fieldsSubset){ this.fieldsSubset = new Fields(fieldsSubset); return this; }
    public SolrBolt withIdFields(String[] idFields){ this.idFields = new Fields(idFields); return this; }
    public SolrBolt withIdFieldsDelimiter(String idFieldsDelimiter){ this.idFieldsDelimiter = idFieldsDelimiter; return this; }
    public SolrBolt withDateConversionFields(String[] dateConversionFields, String dateFormat){ 
    	this.dateConversionFields = new Fields(dateConversionFields);
		this.dateFormat = new SimpleDateFormat(dateFormat);
    	return this;
    	
    }

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector){
		cal = Calendar.getInstance();
		this.collector = collector;
		this.solrServer = new HttpSolrServer(this.solrServerUrl); 
	}

	public void execute(Tuple tuple) {
 
		SolrInputDocument solrDoc = new SolrInputDocument();
		Fields solrOutputFields = null;
		if (fieldsSubset == null){
			solrOutputFields = tuple.getFields();
		}else{
			solrOutputFields = fieldsSubset;
		}
		for (String field : solrOutputFields){
			if (dateConversionFields != null && dateConversionFields.contains(field)){
				try{
					cal.setTime(dateFormat.parse(tuple.getStringByField(field)));
					solrDoc.addField(field + "_dt", solrDateFormat.format(cal.getTime()));
				}catch (ParseException e){
					System.out.println("SolrBolt: Invalid date format for field: " + field + " ignoring this field for this row");
				}
			}else{
				solrDoc.addField(field + getSolrTypeSuffix(tuple.getValueByField(field)), tuple.getValueByField(field));
			}
		}
		if (fixedValueFieldName.length() > 0 && fixedValueFieldValue.length() >0){
			solrDoc.addField(fixedValueFieldName, fixedValueFieldValue);
		}
		if (idFields != null){
			StringBuffer id = new StringBuffer();
			for (String field : idFields){
				id.append(tuple.getStringByField(field) + this.idFieldsDelimiter);
			}
			solrDoc.addField("id", id);
		}

		try{
			solrServer.add(solrDoc);
			solrServer.commit(false, false);
			collector.ack(tuple);
		} catch (Exception e) {
			System.out.println("Error writing solr document");
			e.printStackTrace();			
		}		

	}
	
    private String getSolrTypeSuffix(Object o){
        if (o instanceof String) return "_s";
        else if (o instanceof Integer) return "_i";
        else if (o instanceof Long) return "_l";
        else if (o instanceof Double) return "_d";
        else if (o instanceof Float) return "_f";
        else if (o instanceof Boolean) return "_b";
        //TODO implement date/timestamp type
        else return null;
      }
    
	public void cleanup() {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}

