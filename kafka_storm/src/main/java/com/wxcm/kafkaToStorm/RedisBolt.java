package com.wxcm.kafkaToStorm;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.storm.shade.org.apache.commons.io.FileUtils;

import com.wxcm.utils.DateUtils;
import com.wxcm.utils.RedisUtils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class RedisBolt extends BaseRichBolt {

	String redisClient = "192.168.1.250";
	String port = "6379";

	public String data = null;
	Map stormConf;
	TopologyContext context;
	OutputCollector collector;

	String sdmac = null;
	String provinceCode = null;
	String cityCode = null;
	String areaCode = null;
	String agentId = null;
	String currentDate = null;
	Date date = null;
	int currentMillits = 0;
	String redisKey = null;
	String redisCityKey = null;
	String redisAreaKey = null;
	List<String> agentIdList = null;
	String captureTime = null;
	int todayStartSeconds  = 0;
	int todayEndSeconds  = 0;
	String[] split = new String[]{};
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -3064735625543543449L;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.stormConf = stormConf;
		this.context = context;
		this.collector = collector;

	}

	public void execute(Tuple tuple) {
		try {
			data = tuple.getString(0);
			if (null != data && data.length() > 0) {
				split = data.split("\t");
				if (split[0].equals("YT1013")) {
					sdmac = split[6];
					provinceCode = split[22];
					cityCode = split[23];
					areaCode = split[3];
					if(provinceCode.length() == 6&& cityCode.length() == 6 && areaCode.length() == 6){
						captureTime = split[9];
						if(captureTime.matches( "^-?\\d+$")&& captureTime.length()<=10){
							//判断一下，如果captureTime是在今天的范围内，那么就进行计算，否则就不计算了
							todayStartSeconds = DateUtils.getTodayStartSeconds();
							todayEndSeconds = DateUtils.getTodayEndSeconds();
						//	if(Integer.parseInt(captureTime)>todayStartSeconds  && Integer.parseInt(captureTime)<todayEndSeconds){
								agentIdList=  RedisUtils.getInstance().getAgentIdBySdmac(sdmac.toUpperCase(), redisClient, port);
								agentId =agentIdList.get(0);
								date = new Date();
								currentDate = DateUtils.dateToStr(date, "yyyyMMddHH");
								currentMillits = DateUtils.getMinute(date);
								redisKey = agentId+"_"+provinceCode+"_"+currentDate+currentMillits;
								redisCityKey =  agentId+"_"+provinceCode+"_"+cityCode+"_"+currentDate+currentMillits;
								redisAreaKey =  agentId+"_"+provinceCode+"_"+cityCode+"_"+areaCode+"_"+currentDate+currentMillits;
								
								RedisUtils.getInstance().putSdmacToAgentId(sdmac, redisKey, redisClient, port);
								RedisUtils.getInstance().putSdmacToAgentId(sdmac, redisCityKey, redisClient, port);
								RedisUtils.getInstance().putSdmacToAgentId(sdmac, redisAreaKey, redisClient, port);
						//	}
								collector.emit(new Values(data));
						}
					}
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("lines"));
	}
	

}
