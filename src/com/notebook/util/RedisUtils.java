package com.notebook.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtils {
	
	private static int SECOND = 3600*12;//KEY的国旗时间，单位为秒
	private static final String HOST = "192.168.50.100";
	private static final int PORT = 6379;
	private static final String AUTH = "123";
	private static JedisPool pool;
	
	static{
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxIdle(1000);//空闲时最大连接
		config.setMaxTotal(10000);//最大连接
		config.setMinIdle(500);//空闲时最少
		if (pool==null) {
			pool = new JedisPool(config, HOST, PORT,2000);
		}
	}
	
	public static Jedis getJedis(){
		Jedis jedis = pool.getResource();
		jedis.auth(AUTH);
		return  jedis;
	}
	
	public static void closeJedis(Jedis jedis){
		jedis.close();
	}
	
	/**
	 * 存储String类型的数据
	 * @param key
	 * @param value
	 */
	public static void set(String key,String value){
		Jedis jedis = RedisUtils.getJedis();
		if (jedis.exists(key)) {
			jedis.del(key);
		}
		jedis.set(key, value);
		jedis.expire(key, SECOND);
		RedisUtils.closeJedis(jedis);
	}
	
	/**
	 * 存储String类型的数据
	 * @param key
	 * @param value
	 */
	public static void set(String key,String value,int exp){
		Jedis jedis = RedisUtils.getJedis();
		if (jedis.exists(key)) {
			jedis.del(key);
		}
		jedis.set(key, value);
		jedis.expire(key, exp);
		RedisUtils.closeJedis(jedis);
	}

	/**
	 * 根据key获取字符串类型数据
	 * @param key
	 * @return
	 */
	public static String get(String key){
		Jedis jedis = RedisUtils.getJedis();
		String value = null;
		if (jedis.exists(key)) {
			value = jedis.get(key);
			return value;
		}
		RedisUtils.closeJedis(jedis);
		return value;
	}
	
	/**
	 * 存储List类型key
	 * @param key
	 * @param value
	 */
	public static void lset(String key,String ...value){
		Jedis jedis = RedisUtils.getJedis();
		if (jedis.exists(key)) {
			jedis.del(key);
		}
		jedis.lpush(key, value);
		RedisUtils.closeJedis(jedis);
	}

	/**
	 * 根据键获取list
	 * @param key
	 * @return
	 */
	public static List<String> lget(String key){
		Jedis jedis = RedisUtils.getJedis();
		List<String> list = new ArrayList<String>();
		if (jedis.exists(key)) {
			for (String v : jedis.lrange(key, 0, -1)) {
				list.add(v);
			}
			RedisUtils.closeJedis(jedis);
			return list;
		}
		RedisUtils.closeJedis(jedis);
		list.add("");
		return list;
	}
	
	public static long lcount(String key){
		Jedis jedis = RedisUtils.getJedis();
		long count = 0;
		if (jedis.exists(key)) {
			for (String v : jedis.lrange(key, 0, -1)) {
				count++;
			}
			RedisUtils.closeJedis(jedis);
			return count;
		}
		RedisUtils.closeJedis(jedis);
		return 0;
	}
	
	/** 
	 * 根据key存储Set类型
	 * @param key
	 * @param value
	 * 
	 */
	public static void sset(String key,String[] value){
		Jedis jedis = RedisUtils.getJedis();
		if (jedis.exists(key)) {
			jedis.del(key);
		}
		jedis.sadd(key, value);
		RedisUtils.closeJedis(jedis);
	}
	
	/**
	 * 根据键获取Set
	 * @param key
	 * @return
	 */
	public static Set<String> sget(String key){
		Jedis jedis = RedisUtils.getJedis();
		Set<String> set = new HashSet<String>();
		if (jedis.exists(key)) {
			set = jedis.smembers(key);
			RedisUtils.closeJedis(jedis);
			return set;
		}
		RedisUtils.closeJedis(jedis);
		set.add("");
		return set;
	}

	/**
	 * 添加map类型
	 * @param key
	 * @param value
	 */
	public static void hset(String key,Map<String, String>value){
		Jedis jedis = RedisUtils.getJedis();
		if (jedis.exists(key)) {
			jedis.del(key);
		}
		Set<String> k = value.keySet();
		for (String k1 : k) {
			jedis.hset(key, k1, value.get(k1));
		}
		RedisUtils.closeJedis(jedis);
	}
	
	/**
	 * 根据key获取map
	 * @param key
	 * @return
	 */
	@SuppressWarnings("null")
	public static Map<String, String> hget(String key){
		Jedis jedis = RedisUtils.getJedis();
		Map<String, String> map = null;
		if (jedis.exists(key)) {
			map = jedis.hgetAll(key);
			RedisUtils.closeJedis(jedis);
			return map;
		}
		map.put(" ", " ");
		RedisUtils.closeJedis(jedis);
		return map;
	}

	
}
