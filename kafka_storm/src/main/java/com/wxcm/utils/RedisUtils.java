package com.wxcm.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Test;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtils {
	
	
	private static RedisUtils redisUtils ;
	public static JedisPool jedisPool = null;
	private static  List<String> agentIdList = null;
	private static GenericObjectPoolConfig poolConfig = null;
	private static Set<String> wxidSet = null;
	private static Jedis jedis = null;
	
	private RedisUtils(){
		if(this.jedisPool==null){
			poolConfig =  new JedisPoolConfig();
			// 设置最大空闲连接数
			poolConfig.setMaxIdle(200);
			// 连接池中最大连接数
			poolConfig.setMaxTotal(2000);
			// 在获取链接的时候设置的超时时间
			poolConfig.setMaxWaitMillis(3000);
			// 设置连接池在创建连接的时候，需要对新创建的连接进行测试，保证连接池中的连接都是可用的
			poolConfig.setTestOnBorrow(false);
			jedisPool = new JedisPool(poolConfig, "192.168.1.250", 6379,100000);
		}
	}
	
	
	
	public static synchronized  RedisUtils getInstance() {
        if (redisUtils == null) {
        	redisUtils = new RedisUtils();
        }
        return redisUtils;
    }

	public static synchronized Jedis getJedis(String redisClient, String redisPort) {
		if (jedisPool == null) {
			poolConfig =  new JedisPoolConfig();
			// 设置最大空闲连接数
			poolConfig.setMaxIdle(200);
			// 连接池中最大连接数
			poolConfig.setMaxTotal(2000);
			// 在获取链接的时候设置的超时时间
			poolConfig.setMaxWaitMillis(3000);
			// 设置连接池在创建连接的时候，需要对新创建的连接进行测试，保证连接池中的连接都是可用的
			poolConfig.setTestOnBorrow(false);
			jedisPool = new JedisPool(poolConfig, redisClient, Integer.parseInt(redisPort));
		}
		return jedisPool.getResource();
	}

	/**
	 * 把连接放到连接池中
	 * 
	 * @param jedis
	 */
	public synchronized static void returnJedis(Jedis jedis) {
		if(null != jedisPool){
			jedisPool.returnResourceObject(jedis);	
		}
	}
	public static void jedisForset(Jedis jedis, String jedisKey, String jedisData) {
		try {
			Long sadd = jedis.sadd(jedisKey, jedisData);
		} catch (Exception e) {
			if(null != jedisPool){
				jedisPool.returnBrokenResource(jedis);	
			}else{
				jedis.quit();
			}
		}finally{
			try {
				if(null != jedisPool){
					jedisPool.returnResource(jedis);
				}else{
					jedis.quit();
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public synchronized static void  putSdmacToAgentId(String sdmac, String redisKey, String redisClient, String redisPort) {
		try {
			jedis = getJedis(redisClient, redisPort);
			jedis.auth("hadoop");
			jedis.sadd(redisKey, sdmac);
			jedis.expire(redisKey, 7200);
		} catch (Exception e) {
			if(null != jedisPool){
				jedisPool.returnBrokenResource(jedis);
			}else{
				jedis.quit();
			}
			e.printStackTrace();
			// TODO: handle exception
		}finally{
			try {
				if(null != jedisPool){
					jedisPool.returnResource(jedis);
				}else{
					jedis.quit();
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	public synchronized static List<String > getAgentIdBySdmac(String sdmac,String redisClient,String redisPort){
		try {
			jedis = getJedis(redisClient, redisPort);
			jedis.auth("hadoop");
			agentIdList = jedis.hmget("commonDevice", sdmac);
		} catch (Exception e) {
			if(null != jedisPool){
				jedisPool.returnBrokenResource(jedis);
			}else{
				jedis.quit();
			}
			e.printStackTrace();
		}finally{
			try {
				if(null != jedisPool){
					jedisPool.returnResource(jedis);
				}else{
					jedis.quit();
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return agentIdList;
	}

	
	//通过一个wxid和一个距离来获取该wxid到该距离所有的wxid
	
	/**
	 * 
	 * @param wxid  传入的wxid
	 * @param distance  计算的距离
	 * @param redisDB 选择的redis库
	 * @param redisClient  redis的ip
	 * @param redisPort redis的端口
	 * @param redisAuth redis的密码
	 * @return
	 */
	public  synchronized static Set<String> getDistanceWxid(String wxid,int distance ,int redisDB,String redisClient,String redisPort,String redisAuth){
		try {
			jedis = getJedis(redisClient, redisPort);
			jedis.auth(redisAuth);
			jedis.select(redisDB);
			wxidSet = jedis.smembers(wxid + "_" + distance);
		} catch (Exception e) {
			if(null != jedisPool){
				jedisPool.returnBrokenResource(jedis);
			}else{
				jedis.close();
			}
			e.printStackTrace();
		}finally{
			try {
				if(null != jedisPool){
					jedisPool.returnResource(jedis);
				}else{
					jedis.close();
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return wxidSet;
	}
	
	
	public static void putWxidNearUmac(String wxid,int redisDB,String umacDetail,String redisClient,String redisPort,String redisAuth){
		try {
			jedis = getJedis(redisClient, redisPort);
			jedis.auth(redisAuth);
			jedis.select(redisDB);
			jedis.sadd(wxid, umacDetail);
			jedis.expire(wxid, 7200);
		} catch (Exception e) {
			if(null != jedisPool){
				jedisPool.returnBrokenResource(jedis);	
			}else{
				jedis.close();
			}
			e.printStackTrace();
		}finally{
			try {
				if(null != jedisPool){
					jedisPool.returnResource(jedis);
				}else{
					jedis.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	

	public static void main(String[] args) {
		RedisUtils.putWxidNearUmac("bbb", 3, "bbbbbbccccc", "192.168.1.250", "6379", "hadoop");

	}

}
