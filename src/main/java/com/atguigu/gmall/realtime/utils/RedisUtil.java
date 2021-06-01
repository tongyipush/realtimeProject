package com.atguigu.gmall.realtime.utils;

import com.google.errorprone.annotations.Var;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * 旁路缓存
 * 获取redis的连接池，获取jedis
 */
public class RedisUtil {
    private static JedisPool jedisPool;

    //todo 建立数据库连接池，连接池配置参数
    public static Jedis getJedis(){
        if (jedisPool == null){
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            //todo 连接池总连接数
            jedisPoolConfig.setMaxTotal(100);
            //todo 连接池最大最小空闲
            jedisPoolConfig.setMaxIdle(30);
            jedisPoolConfig.setMinIdle(20);
            //todo 资源消耗尽等待时间
            jedisPoolConfig.setBlockWhenExhausted(true);
            jedisPoolConfig.setMaxWaitMillis(5000);
            //todo 从连接池连接进行测试
            // 导致连接池连接断掉的原因
            // 服务器重启过 网断过 服务器端维持空闲连接超时
            jedisPoolConfig.setTestOnBorrow(true);

            jedisPool = new JedisPool(jedisPoolConfig,"hadoop102",6379,10000);
        }


        System.out.println("--------连接池Redis--------");
        return jedisPool.getResource();
    }
}
