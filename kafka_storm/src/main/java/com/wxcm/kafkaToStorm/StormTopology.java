package com.wxcm.kafkaToStorm;

import java.util.UUID;

import com.google.common.collect.ImmutableList;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class StormTopology {
	/*private static  String zookeeper = "";
	private  static String groupId="";
	private static String topic="";*/
	private static String kafkaZookeeper = null;
	private static BrokerHosts brokerHosts = null;
	private static Config stormConfi =null;
	private static SpoutConfig config =null;
	
	
	public static void main(String[] args){
		try {
		kafkaZookeeper = "hadoop1:2181,hadoop2:2181,hadoop3:2181";
		brokerHosts =new ZkHosts(kafkaZookeeper);
		config = new SpoutConfig(brokerHosts, "auditdata", "/auditdata",  UUID.randomUUID().toString());
		//config.scheme = new SchemeAsMultiScheme(new StringScheme());
		config.scheme = new backtype.storm.spout.SchemeAsMultiScheme(new StringScheme());
		config.zkServers =  ImmutableList.of("hadoop1","hadoop2","hadoop3");
		config.zkPort = 2181;
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafkaSpout", new KafkaSpout(config),3);
		builder.setBolt("redisBolt", new RedisBolt(),3).setNumTasks(3).shuffleGrouping("kafkaSpout");
		builder.setBolt("locateBolt", new RealLocateBolt(),6).setNumTasks(6).shuffleGrouping("redisBolt");
		stormConfi= new Config();
		if (args != null && args.length > 0) {
			stormConfi.setDebug(false);
			stormConfi.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 60000); 
			stormConfi.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 10);  
			stormConfi.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, 1000);  
			stormConfi.put(Config.TOPOLOGY_ACKER_EXECUTORS, 3);  
			stormConfi.setNumWorkers(3);
			stormConfi.setMaxTaskParallelism(100);
			stormConfi.setMessageTimeoutSecs(300);
			stormConfi.setMaxSpoutPending(5000);
			StormSubmitter.submitTopology(args[0], stormConfi, builder.createTopology());
		} else {
			stormConfi.setDebug(false);
			stormConfi.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 60000); 
			stormConfi.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 10);  
			stormConfi.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, 1000);  
			stormConfi.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);  
			//stormConfi.put("nimbus.host", "hadoop1");  
			stormConfi.setNumWorkers(3);
			stormConfi.setMaxTaskParallelism(100);
			stormConfi.setMessageTimeoutSecs(300);
			stormConfi.setMaxSpoutPending(5000);
			stormConfi.setDebug(true);
			backtype.storm.LocalCluster cluster = new backtype.storm.LocalCluster();
			cluster.submitTopology("special-topology", stormConfi, builder.createTopology());
			Thread.sleep(500000);
			cluster.shutdown();
		} 
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
