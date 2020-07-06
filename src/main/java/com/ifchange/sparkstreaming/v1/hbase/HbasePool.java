package com.ifchange.sparkstreaming.v1.hbase;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class HbasePool {

    private int initialConnections = 3; // 连接池的初始大小
    private int incrementalConnections = 4;// 连接池自动增加的大小
    private int maxConnections = 64; // 连接池最大的大小
    private BlockingQueue<HbaseClient> hbases = new LinkedBlockingQueue<>(maxConnections); // 存放连接池中数据库连接的向量
    private int totalCount = 0;//总共连接数
    private AtomicInteger currentCount = new AtomicInteger(0);//当前可用连接数
    private String hosts = null; // hbase zk主机名（多个以逗号隔开）
    private String port = null; // hbase zk 端口号
    private String tableName = null; // hbase表名

    public HbasePool(String tablename) throws Exception {
        tableName = tablename;
        initHbaseConn();
    }

    public HbasePool(String tablename, String zkhosts) throws Exception {
        tableName = tablename;
        hosts = zkhosts;
        initHbaseConn();
    }

    public HbasePool(String tablename, String zkhosts, String zkport) throws Exception {
        tableName = tablename;
        hosts = zkhosts;
        port = zkport;
        initHbaseConn();
    }

    public void setMaxNumber(int number) {
        if (number <= initialConnections) {
            return;
        }
        maxConnections = number;
        hbases = new LinkedBlockingQueue<>(maxConnections);
        currentCount = new AtomicInteger(0);
        totalCount = 0;
        try {
            initHbaseConn();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void setAutoIncreaseNumber(int number) {
        incrementalConnections = number;
    }

    public HbaseClient getHbaseClient() throws  Exception {
        if (currentCount.get() < 1) {
            addHbaseConn();
        }
        return take();
    }

    private synchronized void addHbaseConn() throws  Exception {
        for (int i = 0; i < incrementalConnections && totalCount < maxConnections; i++) {
            hbases.put(createHbaseClient());
            currentCount.addAndGet(1);
            totalCount++;
        }
    }

    private void initHbaseConn() throws Exception {
        for (int i = 0; i < initialConnections; i++) {
            hbases.put(createHbaseClient());
            currentCount.addAndGet(1);
            totalCount++;
        }
    }

    private HbaseClient createHbaseClient() throws Exception {
        HbaseClient hbaseClient;
        if (hosts != null && port != null) {
            hbaseClient = new HbaseClient(hosts, port);
            hbaseClient.setTbale(tableName);
        } else if (hosts != null) {
            hbaseClient = new HbaseClient(hosts);
            hbaseClient.setTbale(tableName);
        } else {
            hbaseClient = new HbaseClient();
            hbaseClient.setTbale(tableName);
        }
        return hbaseClient;
    }

    private HbaseClient take() throws InterruptedException {
        currentCount.decrementAndGet();
        HbaseClient hb = hbases.take();
        hb.busy();
        return hb;
    }

    private void put(HbaseClient hb) throws InterruptedException {
        if (hb == null) {
            throw new NullPointerException("HbaseClient is null");
        }
        hb.free();
        hbases.put(hb);
        currentCount.addAndGet(1);
    }

    public void free(HbaseClient hbaseClient) throws InterruptedException {
        put(hbaseClient);
        //System.out.println(Thread.currentThread().getName().concat(" ")+ "free hbase connect, now have " + hbases.size() + ":"+ String.valueOf(currentCount.get()) + " connect");
    }

    public void close() throws Exception {
        while (totalCount > 0) {
            for (HbaseClient hbase : hbases) {
                hbase.close();
                totalCount--;
                if (totalCount < 1) {
                    break;
                }
                //System.out.println("have " + totalCount + " connect need close");
            }
        }
        //System.out.println("hbasePool close success......");
    }

    public int getCurrentCount() {
        return currentCount.get();
    }

    public int getTotalCount() {
        return totalCount;
    }

}
