package com.github.gbraccialli.telco.cdr.storm;

import java.io.FileReader;
import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.github.gbraccialli.telco.cdr.storm.bolt.NetworkTypeChange;
import com.github.randerzander.StormCommon.bolts.RollingCountFromTimestampBolt;
import com.github.randerzander.StormCommon.bolts.SolrBolt;
import com.github.randerzander.StormCommon.bolts.SysoutBolt;
import com.github.randerzander.StormCommon.utils.CSVScheme;
import com.github.randerzander.StormCommon.utils.Utils;


public class Topology{
	public static void main(String[] args) throws Exception {

		//Read props file into HashMap
		HashMap<String, String> props = getPropertiesMap(args[0]);
		String topologyName = props.get("topologyName");
		if (Utils.checkProp(props, "killIfRunning", "true") && Utils.checkProp(props, "localMode", "false")) {
			Utils.killTopology(topologyName);
		}
		String zkHost = props.get("zk.host");
		String zkRoot = (props.get("zk.root") == null) ? "/kafkastorm" : props.get("zk.root");
		String dateFormat = props.get("dateFormat");
		String solrServerUrl = props.get("solrServerUrl");


		Config conf = new Config();
		conf.setNumWorkers(Integer.parseInt(props.get("numWorkers")));
		/*
    HashMap<String, String> hikariProps = new HashMap<String, String>();
    hikariProps.put("jdbcUrl", props.get("jdbcUrl"));
    conf.put("jdbc.conf", hikariProps);
		 */

		TopologyBuilder builder = new TopologyBuilder();

		final String[] kafkaFields = new String[]{ 
				"session_id",
				"sim_card_id",
				"phone_number",
				"record_opening_time",
				"duration",
				"cell_id",
				"network_type"
		};

		final String[] idsFieldsSolrNetworkTypeChangeRaw = new String[] {"session_id", "record_opening_time"};
		final String[] dateFieldsSolrNetworkTypeChangeRaw = new String[] {"record_opening_time"};

		final String[] keysFieldsRollingCountNetworkTypeChange = new String[] {"cell_id_from", "cell_id_to", "network_type_change"};
		final String[] idsFieldsSolrNetworkTypeChangeRollingCount = new String[] {"cell_id_from", "cell_id_to", "network_type_change", "record_opening_time_rounded"};
		final String[] dateFieldsSolrNetworkTypeChangeRollingCount = new String[] {"record_opening_time_rounded"};


		SpoutConfig CDRConfig = new SpoutConfig(new ZkHosts(zkHost),props.get("CDR.topic"), zkRoot, UUID.randomUUID().toString());
		CDRConfig.scheme = new SchemeAsMultiScheme(new CSVScheme(kafkaFields,","));
		CDRConfig.forceFromStart = (Utils.checkProp(props, "CDR.fromBeginning", "true"));
		builder.setSpout("CDRSpout", new KafkaSpout(CDRConfig));

		builder.setBolt("networkTypeChangeBolt", new NetworkTypeChange("session_id", "cell_id", "network_type", "record_opening_time", dateFormat))
			.fieldsGrouping("CDRSpout", new Fields("session_id"));

		SolrBolt solrBoltNetworkdTypeChangeRaw = new SolrBolt()
			.withFixedValueField("docType_s", "network_type_change_raw")
			.withIdFields(idsFieldsSolrNetworkTypeChangeRaw)
			.withDateConversionFields(dateFieldsSolrNetworkTypeChangeRaw, dateFormat)
			.withSolrServer(solrServerUrl)
		;
		//builder.setBolt("solrNetworkTypeChangeRaw", solrBoltNetworkdTypeChangeRaw).shuffleGrouping("networkTypeChangeBolt");
		//builder.setBolt("sysoutNetworkdTypeChangeRaw", new SysoutBolt("ChangeAlerts")).shuffleGrouping("networkTypeChangeBolt");

		RollingCountFromTimestampBolt rollingCountBoltNetworkTypeChanging = new RollingCountFromTimestampBolt()
			.withKeyFields(keysFieldsRollingCountNetworkTypeChange)
			.withTimestampField("record_opening_time", dateFormat)
		;
		builder.setBolt("rollingCountBoltNetworkTypeChanging", rollingCountBoltNetworkTypeChanging)
			.fieldsGrouping("networkTypeChangeBolt", new Fields(keysFieldsRollingCountNetworkTypeChange));

		SolrBolt solrBoltNetworkdTypeChangeRollingCount = new SolrBolt()
			.withFixedValueField("docType_s", "network_type_change_rolling_count")
			.withIdFields(idsFieldsSolrNetworkTypeChangeRollingCount)
			.withDateConversionFields(dateFieldsSolrNetworkTypeChangeRollingCount, dateFormat)
			.withSolrServer(solrServerUrl)
		;
		builder.setBolt("solrNetworkTypeChangeRollingCount", solrBoltNetworkdTypeChangeRollingCount).shuffleGrouping("rollingCountBoltNetworkTypeChanging");
		builder.setBolt("sysoutNetworkdTypeChangeRollingCount", new SysoutBolt("rollingCountBoltNetworkTypeChanging")).shuffleGrouping("rollingCountBoltNetworkTypeChanging");

		System.out.println("going to submit Topology:" + topologyName); 

		//Submit topology
		if (Utils.checkProp(props, "localMode", "true")) 
			new LocalCluster().submitTopology(topologyName, conf, builder.createTopology());
		else 
			StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, builder.createTopology());
	}

	//Converts topology.properties file to a HashMap of key value configuration pairs
	public static HashMap<String, String> getPropertiesMap(String file){
		Properties props = new Properties();
		try{ props.load(new FileReader(file)); }
		catch(Exception e){ e.printStackTrace(); System.exit(-1); }

		HashMap<String, String> map = new HashMap<String, String>();
		for (final String name: props.stringPropertyNames()) map.put(name, (String)props.get(name));
		return map;
	}
}
