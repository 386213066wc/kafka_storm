package com.wxcm.kafkaToStorm;

import java.util.Date;
import java.util.Map;
import java.util.Set;

import com.wxcm.utils.DateUtils;
import com.wxcm.utils.RedisUtils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class RealLocateBolt extends BaseRichBolt{
	private String line = null;
	private String[] split = null;
	
	private String umac= null;
	private String wxid = null;
	private String longtitude = null;
	private String latitude = null;
	private String currentDate = null;
	private Date date = null;
	private int currentMillits = 0;
	
	private Set<String> redisWxid = null;
	private String redisIp = "192.168.1.250";
	private String redisPort = "6379";
	private String redisAuth = "hadoop";
	
	Map stormConf;
	TopologyContext context;
	OutputCollector collector;
	private int[] distanceCalc = new int[]{100,300,500,1000,3000};
	private int calc = 0;
	/**
	 * 
	 */
	private static final long serialVersionUID = -1782161060556904207L;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.stormConf = stormConf;
		this.context =  context;
		this.collector= collector;
	}


	public void execute(Tuple tuple) {
		try {
			line = tuple.getString(0);
			if (null != line && line.length() > 0) {
				split=line.split("\t");
				if(split[0].equals("YT1013")){
					/**
					 * 	firstType=100
						secondType=300
						thirdType=500
						fourthType=1000
						fifthType=3000
					 */
					//获取到每条数据的umac，wxid，longtitude，latitude
					//然后取这个wxid周边100米，300米，500米，1000米，3000米的所有wxid
					//  YT1013=iumac,idmac,area_code,policeid,sumac,sdmac,datasource,netsite_type,capture_time,netbar_wacode,brandid,cache_ssid,terminal_filed_strength,ssid_position,access_ap_mac,access_ap_channel,access_ap_encryption_type,collection_equipment_id,collection_equipment_longitude,collection_equipment_latitude,wxid,province_code,city_code,typename,security_software_orgcode
					wxid=split[21];
					umac = split[5];
					longtitude = split[19];
					latitude = split[20];
					for(int i =0;i<distanceCalc.length;i++){
						calc = distanceCalc[i];
						redisWxid = RedisUtils.getInstance().getDistanceWxid(wxid, calc, 2, redisIp, redisPort,redisAuth);
						date = new Date();
						currentDate = DateUtils.dateToStr(date, "yyyyMMddHH");
						currentMillits = DateUtils.getMinute(date);
						for (String wxid : redisWxid) {
							//这个集合里面存的就是距离该wxid一百米范围内的所有的wxid
							RedisUtils.getInstance().putWxidNearUmac(wxid+"_"+calc+"_"+currentDate+currentMillits, 3 , umac+"_"+longtitude+"_"+latitude, redisIp, redisPort, redisAuth);
						}
					}
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		
	}


	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	

}
