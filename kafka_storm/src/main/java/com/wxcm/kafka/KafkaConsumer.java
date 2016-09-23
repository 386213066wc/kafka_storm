package com.wxcm.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;



public class KafkaConsumer {
	private ConsumerConnector consumer;

	private KafkaConsumer() {
		Properties props = new Properties();
		/**
		 * zookeeper.session.timeout.ms=5000
		 * zookeeper.connection.timeout.ms=10000 zookeeper.sync.time.ms=2000
		 */
		props.put("zookeeper.connect", "hadoop1:2181,hadoop2:2181,hadoop3:2181");
		props.put("group.id", "test");
		props.put("zookeeper.session.timeout.ms", "4000");
		props.put("zookeeper.connection.timeout.ms", "10000");
		props.put("zookeeper.sysnc.time.ms", "2000");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "smallest");
		// kafka.common.ConsumerRebalanceFailedException异常解决办法
		props.put("rebalance.max.retries", "5");
		props.put("rebalance.backoff.ms", "1200");
		// 序列化类
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		ConsumerConfig configs = new ConsumerConfig(props);
		ConsumerConfig config = new ConsumerConfig(props);
		
		consumer = Consumer.createJavaConsumerConnector(config);
	}

	void consume() {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		// 这里map接收两个类型，第一个是topic，接收主题，第二个是线程数量，即使用多少个线程来进行consumer端的消费。
		topicCountMap.put(KafkaProducer.TOPIC, new Integer(1));
		StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
		StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());
		Map<String, List<KafkaStream<String, String>>> consumerMap = consumer.createMessageStreams(topicCountMap,
				keyDecoder, valueDecoder);
		KafkaStream<String, String> stream = consumerMap.get(KafkaProducer.TOPIC).get(0);
		ConsumerIterator<String, String> it = stream.iterator();
		while (it.hasNext()) {
			System.out.println(it.next().message());
			// 消费完成之后清理日志，避免重复消费的情况
			 it.clearCurrentChunk();
		}
	}

	
	
	void myConsume(){
		
		  Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
	        topicCountMap.put(KafkaProducer.TOPIC, new Integer(1));

	        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
	        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

	        Map<String, List<KafkaStream<String, String>>> consumerMap = 
	                consumer.createMessageStreams(topicCountMap,keyDecoder,valueDecoder);
	        KafkaStream<String, String> stream = consumerMap.get(KafkaProducer.TOPIC).get(0);
	        ConsumerIterator<String, String> it = stream.iterator();
	        while (it.hasNext()){
	        	System.out.println(it.next().message());
	        //	it.clearCurrentChunk();
	        }
	            
		
		
	}
	
	
	
	
	public static void main(String[] args) {
		new KafkaConsumer().myConsume();
	}

}
