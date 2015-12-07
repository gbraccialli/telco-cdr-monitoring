package com.github.gbraccialli.storm.commom.utils;

import java.io.FileReader;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.TopologySummary;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.NimbusClient;

public class Utils{
  public static boolean isTickTuple(Tuple tuple) {
    return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
  }

  public static void killTopology(String topologyName){
    Client client = NimbusClient.getConfiguredClient(backtype.storm.utils.Utils.readStormConfig()).getClient();
    if (topologyRunning(client, topologyName)){
      System.out.print(topologyName + " is currently running. Killing..");
      try{
        KillOptions opts = new KillOptions();
        opts.set_wait_secs(0);
        client.killTopologyWithOpts(topologyName, opts);
      }catch(Exception e){}
      while (topologyRunning(client, topologyName)){
        System.out.print(".");
        try{ Thread.sleep(1000); } catch(Exception e){}
      }
      System.out.println("killed.");
    }
  }

  public static boolean topologyRunning(Client client, String topologyName){
    try{
    for (TopologySummary topology: client.getClusterInfo().get_topologies())
      if (topology.get_name().equals(topologyName)) return true;
    }catch(Exception e){ e.printStackTrace(); }
    return false;
  }

  public static String[] getFields(HashMap<String, String> props, String componentName){
    String prefix = componentName + ".";
    String tmp = null;
    if (props.get(prefix+"fields") != null) tmp = props.get(prefix+"fields");
    return (tmp != null) ? tmp.split(",") : null;
  }

  public static boolean checkProp(HashMap<String, String> props, String prop, String val){
    if ((val == null) && (props.get(prop) == null)) return true;
    if ((val != null) && (props.get(prop) == null)) return false;
    if ((val != null) && (props.get(prop).equals(val))) return true;
    return false;
  }

  public static int getParallelism(HashMap<String, String> props, String componentName){
    String prefix = componentName + ".";
    if (props.get(prefix+"parallelism") != null) return Integer.parseInt(props.get(prefix+"parallelism"));
    else return 1;
  }

  public static HashMap<String, String> getPropertiesMap(String file){
    Properties props = new Properties();
    try{ props.load(new FileReader(file)); }
    catch(Exception e){ e.printStackTrace(); System.exit(-1); }

    HashMap<String, String> map = new HashMap<String, String>();
    for (final String name: props.stringPropertyNames()) map.put(name, (String)props.get(name));
    return map;
  }

  public static void run(TopologyBuilder builder, String topologyName, Config conf, boolean killIfRunning, boolean localMode) {
    if (killIfRunning) killTopology(topologyName);
    if (localMode) new LocalCluster().submitTopology(topologyName, conf, builder.createTopology());
    else{ 
      try { StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, builder.createTopology()); }
      catch (Exception e) { e.printStackTrace(); throw new RuntimeException(e); }
    }
  }

  public static ArrayList<Object> ArrayListFromRow(ResultSet results, int columns){
    try{
      ArrayList<Object> row = new ArrayList<Object>();
      for (int i = 0; i < columns; i++) row.add(results.getObject(i+1));
      return row;
    }catch (Exception e) { e.printStackTrace(); throw new RuntimeException(e); }
  }

  public static String[] getTypes(ResultSet results){
    try{
      ResultSetMetaData md = results.getMetaData();
      String[] types = new String[md.getColumnCount()];
      for (int i = 0; i < types.length; i++) types[i] = md.getColumnTypeName(i+1);
      return types;
    }catch (Exception e) { e.printStackTrace(); throw new RuntimeException(e); }
  }

}
