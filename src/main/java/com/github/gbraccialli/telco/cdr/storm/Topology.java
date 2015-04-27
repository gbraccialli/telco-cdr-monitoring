package com.github.gbraccialli.telco.cdr.storm;

import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.storm.guava.collect.Lists;
import org.apache.storm.hive.bolt.HiveBolt;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.common.HiveOptions;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.github.gbraccialli.telco.cdr.storm.bolt.DroppedCallBolt;
import com.github.gbraccialli.telco.cdr.storm.bolt.NetworkTypeChangeBolt;
import com.github.randerzander.StormCommon.bolts.RollingCountFromTimestampBolt;
import com.github.randerzander.StormCommon.bolts.SolrBolt;
import com.github.randerzander.StormCommon.bolts.SysoutBolt;
import com.github.randerzander.StormCommon.utils.CSVScheme;
import com.github.randerzander.StormCommon.utils.Utils;


public class Topology{
	public static void main(String[] args) throws Exception {

		//Read props file into HashMap
		HashMap<String, String> props = getPropertiesMap(args[0]);
		String topologyName = props.get("storm.topologyName");
		if (Utils.checkProp(props, "storm.killIfRunning", "true") && Utils.checkProp(props, "storm.localMode", "false")) {
			Utils.killTopology(topologyName);
		}
		String zkHost = props.get("zookeeper.host");
		String zkRoot = (props.get("zookeeper.root") == null) ? "/kafkastorm" : props.get("zookeeper.root");
		String dateFormat = props.get("dateFormat");
		String solrServerUrl = props.get("solr.serverUrl");
		String hiveMetaStoreURI = props.get("hive.metaStoreURI");
		String hiveDbName = props.get("hive.dbName");
		String hiveTblName = props.get("hive.tblName");
		
		Config conf = new Config();
		conf.setNumWorkers(Integer.parseInt(props.get("storm.numWorkers")));
		
		HashMap<String, String> hikariProps = new HashMap<String, String>();
		hikariProps.put("jdbcUrl", props.get("phoenix.jdbcURL"));
		conf.put("jdbc.conf", hikariProps);

		TopologyBuilder builder = new TopologyBuilder();

		final String[] allFields = props.get("allFields").split(",");
		final String sessionIdField = props.get("sessionIdField");
		final String cellIdField = props.get("cellIdField");
		final String networkTypeField = props.get("networkTypeField");
		final String timestampField = props.get("timestampField");
		final String simcardIdField = props.get("simcardIdField");
		final String phoneNumberField = props.get("phoneNumberField");
		final String dropReasonField = props.get("dropReasonField");

		final String[] keysFieldsRollingCountNetworkTypeChange = new String[] {cellIdField + "_from", cellIdField + "_to", networkTypeField + "_change"};
		final String[] idsFieldsSolrNetworkTypeChangeRollingCount = new String[] {cellIdField + "_from", cellIdField + "_to", networkTypeField + "_change", timestampField +  "_rounded"};
		final String[] dateFieldsSolrNetworkTypeChangeRollingCount = new String[] { timestampField + "_rounded"};

		final String[] keysFieldsRollingCountDroppedCall = new String[] {cellIdField, dropReasonField};
		final String[] idsFieldsSolrDroppedCallRollingCount = new String[] {cellIdField, dropReasonField, timestampField +  "_rounded"};
		final String[] dateFieldsSolrDroppedCallRollingCount = new String[] {timestampField +  "_rounded"};

		//1 SOURCE KAFKA - SPOUT
		SpoutConfig CDRConfig = new SpoutConfig(new ZkHosts(zkHost),props.get("kafka.topic"), zkRoot, UUID.randomUUID().toString());
		CDRConfig.scheme = new SchemeAsMultiScheme(new CSVScheme(allFields,","));
		CDRConfig.forceFromStart = (Utils.checkProp(props, "kafka.fromBeginning", "true"));
		builder.setSpout("CDRSpout", new KafkaSpout(CDRConfig));

		//2 HIVE BOLT - RAW CDR DATA
		DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
        	.withColumnFields(new Fields(allFields));
		HiveOptions hiveOptions = new HiveOptions(hiveMetaStoreURI,hiveDbName,hiveTblName,mapper)
			.withBatchSize(10)
			.withTxnsPerBatch(10);
		HiveBolt hiveBolt = new HiveBolt(hiveOptions);
		builder.setBolt("CDRHiveBolt", hiveBolt).shuffleGrouping("CDRSpout");
		
		//3 NETWORK TYPE CHANGE - DETECTION
		builder.setBolt("networkTypeChangeBolt", new NetworkTypeChangeBolt(sessionIdField, cellIdField, networkTypeField, timestampField, dateFormat))
			.fieldsGrouping("CDRSpout", new Fields(sessionIdField));
		
			//3.A NETWORK TYPE CHANGE - ROLLING COUNT
			RollingCountFromTimestampBolt rollingCountBoltNetworkTypeChanging = new RollingCountFromTimestampBolt()
				.withKeyFields(keysFieldsRollingCountNetworkTypeChange)
				.withTimestampField(timestampField, dateFormat)
			;
			builder.setBolt("rollingCountBoltNetworkTypeChanging", rollingCountBoltNetworkTypeChanging)
				.fieldsGrouping("networkTypeChangeBolt", new Fields(keysFieldsRollingCountNetworkTypeChange));

				//3.A.1 NETWORK TYPE CHANGE - SOLR
				SolrBolt solrBoltNetworkdTypeChangeRollingCount = new SolrBolt()
					.withFixedValueField("docType_s", "network_type_change_rolling_count")
					.withIdFields(idsFieldsSolrNetworkTypeChangeRollingCount)
					.withDateConversionFields(dateFieldsSolrNetworkTypeChangeRollingCount, dateFormat)
					.withSolrServer(solrServerUrl)
				;
				builder.setBolt("solrNetworkTypeChangeRollingCount", solrBoltNetworkdTypeChangeRollingCount).shuffleGrouping("rollingCountBoltNetworkTypeChanging");
				//builder.setBolt("sysoutNetworkdTypeChangeRollingCount", new SysoutBolt("rollingCountBoltNetworkTypeChanging")).shuffleGrouping("rollingCountBoltNetworkTypeChanging");
			
			//3.B PHOENIX
            List<Column> columnSchemaPhoenixNetworkTypeChange = Lists.newArrayList(
            	    new Column(sessionIdField, java.sql.Types.VARCHAR),
            		new Column(timestampField, java.sql.Types.VARCHAR),
            		new Column(cellIdField + "_from", java.sql.Types.VARCHAR),
            	    new Column(cellIdField + "_to", java.sql.Types.VARCHAR),
            	    new Column(networkTypeField + "_change", java.sql.Types.VARCHAR)
            	    );
        	JdbcMapper simpleJdbcMapperPhoenixNetworkTypeChange = new SimpleJdbcMapper(columnSchemaPhoenixNetworkTypeChange);
				
			JdbcInsertBolt phoenixNetworkTypeChange = new JdbcInsertBolt("jdbc.conf", simpleJdbcMapperPhoenixNetworkTypeChange)
               .withInsertQuery("upsert into CDR.NETWORK_TYPE_CHANGE values (?,?,?,?,?)")
               .withQueryTimeoutSecs(0);

			builder.setBolt("phoenixNetworkTypeChange", phoenixNetworkTypeChange)
				.shuffleGrouping("networkTypeChangeBolt");
				
		//4 DROPPED CALLS - DETECTION
		builder.setBolt("droppedCallBolt", new DroppedCallBolt(simcardIdField, phoneNumberField, dropReasonField, cellIdField, timestampField, dateFormat))
		.fieldsGrouping("CDRSpout", new Fields(simcardIdField, phoneNumberField));
		
			//4.A DROPPED CALLS - ROLLING COUNT
			RollingCountFromTimestampBolt rollingCountBoltDroppedCall = new RollingCountFromTimestampBolt()
				.withKeyFields(keysFieldsRollingCountDroppedCall)
				.withTimestampField(timestampField, dateFormat)
			;
			builder.setBolt("rollingCountBoltDroppedCall", rollingCountBoltDroppedCall)
				.fieldsGrouping("droppedCallBolt", new Fields(keysFieldsRollingCountDroppedCall));

				//4.A.1 DROPPED CALLS - SOLR
				SolrBolt solrBoltDroppedCallRollingCount = new SolrBolt()
					.withFixedValueField("docType_s", "dropped_call_rolling_count")
					.withIdFields(idsFieldsSolrDroppedCallRollingCount)
					.withDateConversionFields(dateFieldsSolrDroppedCallRollingCount, dateFormat)
					.withSolrServer(solrServerUrl)
				;
				builder.setBolt("solrDroppedCallRollingCount", solrBoltDroppedCallRollingCount).shuffleGrouping("rollingCountBoltDroppedCall");
				//builder.setBolt("sysoutDroppedCallRollingCount", new SysoutBolt("rollingCountBoltDroppedCall")).shuffleGrouping("rollingCountBoltDroppedCall");
				
			//4.B PHOENIX
            List<Column> columnSchemaPhoenixDroppedCall = Lists.newArrayList(
            	    new Column(simcardIdField, java.sql.Types.VARCHAR),
            	    new Column(phoneNumberField, java.sql.Types.VARCHAR),
            	    new Column(timestampField, java.sql.Types.VARCHAR),
            	    new Column(cellIdField, java.sql.Types.VARCHAR),
            	    new Column(dropReasonField, java.sql.Types.VARCHAR)
            	    );
        	JdbcMapper simpleJdbcMapperPhoenixDroppedCall = new SimpleJdbcMapper(columnSchemaPhoenixDroppedCall);
				
			JdbcInsertBolt phoenixDroppedCall = new JdbcInsertBolt("jdbc.conf", simpleJdbcMapperPhoenixDroppedCall)
               .withInsertQuery("upsert into CDR.DROPPED_CALL values (?,?,?,?,?)")
               .withQueryTimeoutSecs(0);

			builder.setBolt("phoenixDroppedCall", phoenixDroppedCall)
				.shuffleGrouping("droppedCallBolt");
			
		//Submit topology
		System.out.println("going to submit Topology:" + topologyName); 
			
		if (Utils.checkProp(props, "storm.localMode", "true")) 
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
