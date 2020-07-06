package com.ifchange.sparkstreaming.v1.util;

import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RedisCli {

    private static JedisPool pool = null;

    private String host;

    private int port;

    private static JedisPoolConfig poolConfig;

    private static final Logger logger=Logger.getLogger(RedisCli.class);

    static {
        poolConfig = new JedisPoolConfig();
        //设置redis连接池的最大个数
        poolConfig.setMaxTotal(200);
        //设置redis连接池的最大等待个数
        poolConfig.setMaxIdle(10);
        //设置redis连接池的最大等待时间
        poolConfig.setMaxWaitMillis(5000);
    }

    public RedisCli(String host, int port) {
        this.host = host;
        this.port = port;
        initPool();
    }


    /**
     * 同步初始化 JedisPool
     */
    private synchronized void initPool() {
        if (pool == null) {
            pool = new JedisPool(poolConfig, host, port, 2000);
        }
    }

    public  void returnResource(final Jedis jedis) {
        if (pool != null && jedis != null) {
            logger.info("释放redis资源...");
            jedis.close();
        }
    }

    /*
     * 同步获取 Jedis 实例
     * @return Jedis
     */
    public synchronized Jedis getJedis() {
        if (pool == null) {
            initPool();
        }
        return pool.getResource();
    }

    public String get(final String key,final Jedis jedis){
        ReadWriteLock lock=new ReentrantReadWriteLock();
        Lock readLock = lock.readLock();
        readLock.lock();
        String s = null;
        try {
            s = jedis.get(key);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            readLock.unlock();
        }
        return s;
    }

    public void set(final Jedis jedis ,final String key,final String value){
        ReadWriteLock lock=new ReentrantReadWriteLock();
        Lock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            jedis.set(key,value);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            writeLock.unlock();
        }
    }

    public void setPex(final Jedis jedis ,final String key,final String value,final long millSeconds){
        ReadWriteLock lock=new ReentrantReadWriteLock();
        Lock writeLock = lock.writeLock();
        writeLock.lock();
        try {
//            jedis.setex(key,seconds,value);
            jedis.set(key,value);
            jedis.pexpireAt(key,millSeconds);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            writeLock.unlock();
        }
    }

    public void setEx(final Jedis jedis ,final String key,final String value,final long seconds){
        ReadWriteLock lock=new ReentrantReadWriteLock();
        Lock writeLock = lock.writeLock();
        writeLock.lock();
        try {
//            jedis.setex(key,seconds,value);
            jedis.set(key,value);
            jedis.expireAt(key,seconds);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            writeLock.unlock();
        }
    }

    public void incr(final Jedis jedis ,final String key){
        ReadWriteLock lock=new ReentrantReadWriteLock();
        Lock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            jedis.incr(key);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            writeLock.unlock();
        }
    }




}
