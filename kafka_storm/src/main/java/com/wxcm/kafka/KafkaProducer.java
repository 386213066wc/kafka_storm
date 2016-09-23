package com.wxcm.kafka;


import java.util.Properties;

import com.wxcm.utils.Uuid16;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class KafkaProducer {
	public  static final  String TOPIC = "auditdata";
	
	//public static final String TOPIC =   PropertyUtil.getKey("kafkaTopic", "kafka");
	
	
	
	
	Producer<String, String> producer = null;
	//将配置文件引用到spring中去，可以在初始化项目的时候就将文件中的配置文件读取到内存中来。
	/*@Value("${}")
	private  int headerNum;*/
	private KafkaProducer(){
		Properties props  = new Properties();
		//指定kafka连接的zookeeper地址
		props.put("zk.connect", "hadoop1:2181,hadoop2:2181,hadoop3:2181");  
		props.put("metadata.broker.list", "hadoop1:9092,hadoop2:9092,hadoop3:9092");
		props.put("producer.type", "async");
		props.put("compression.codec", "1");
		//配置value的序列化类
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		//配置key的序列化类
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "-1");
		producer = new Producer<String,String>(new ProducerConfig(props));
		
	}
	void produce(){
		int messageNo = 0;
		final int Count = 10;
		while(messageNo<Count){
			String key = String.valueOf(Uuid16.create().toString());
			String data = "hell kafka message"+Uuid16.create().toString();
			producer.send(new KeyedMessage<String, String>(TOPIC, key,data));
			messageNo ++ ;
		}
		producer.close();
	}
	public static void main(String[] args) {
		new KafkaProducer().produce();
	}
}
