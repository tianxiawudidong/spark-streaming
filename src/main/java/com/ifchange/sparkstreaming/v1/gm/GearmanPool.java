/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ifchange.sparkstreaming.v1.gm;


import com.ifchange.sparkstreaming.v1.selib.gearman.SLGearmanClient;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author root
 */
public class GearmanPool {

    private int maxConnections = 32; // 连接池最大的大小
    private BlockingQueue<SLGearmanClient> gms = new LinkedBlockingQueue<>(maxConnections); // 存放连接池中数据库连接的向量
    private AtomicInteger currentCount = new AtomicInteger(0);//当前可用连接数
    private int initialConnections = 8; // 连接池的初始大小
    private int incrementalConnections = 4;// 连接池自动增加的大小
    private int totalCount = 0;//总共连接数
    private String gmConf;
    private static String workName;

    public GearmanPool(String work_name) throws Exception {
//        String path = new File(MysqlConfig.class.getClassLoader().getResource("").getPath()).getParent();
//        String projectname = System.getProperty("user.dir");
//        String pn = projectname.substring(projectname.lastIndexOf("/") + 1, projectname.length());
//        gmConf = String.format("%s/%s/conf/%s", path, pn, GearmanParam.getConfigFileName());
        workName = work_name;
        initGmConn();
    }

    public void setMaxNumber(int number) {
        if (number <= initialConnections) {
            return;
        }
        maxConnections = number;
        gms = new LinkedBlockingQueue<>(maxConnections);
        currentCount = new AtomicInteger(0);
        totalCount = 0;
        try {
            initGmConn();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void setAutoIncreaseNumber(int number) {
        incrementalConnections = number;
    }

    public SLGearmanClient getGmConn() throws InterruptedException, Exception {
        //System.out.println(Thread.currentThread().getName().concat(" ")+ "get gmase connect start, now have " + gms.size() + ":"+ String.valueOf(currentCount.get()) + " connect");
        SLGearmanClient gmaseClient = null;
        if (currentCount.get() < 1 && totalCount < maxConnections) {
            addHbaseConn();
        }
        gmaseClient = take();
        //System.out.println(Thread.currentThread().getName().concat(" ")+ "get gmase connect end, now have " + gms.size() + ":"+ String.valueOf(currentCount.get()) + " connect");
        return gmaseClient;
    }

    private synchronized void addHbaseConn() throws InterruptedException, Exception {
        //System.out.println(Thread.currentThread().getName().concat(" ")+ "gmase connect not enough, create it");
        for (int i = 0; i < incrementalConnections && totalCount < maxConnections; i++) {
            gms.put(createGmConn());
            currentCount.addAndGet(1);
            totalCount++;
        }
    }

    private void initGmConn() throws InterruptedException, Exception {
        for (int i = 0; i < initialConnections; i++) {
            gms.put(createGmConn());
            currentCount.addAndGet(1);
            totalCount++;
        }
    }

    public  SLGearmanClient createGmConn() throws Exception {
        SLGearmanClient gm = new SLGearmanClient(workName);
        gm.start();
        return gm;
    }

    private SLGearmanClient take() throws InterruptedException {
        currentCount.decrementAndGet();
        return gms.take();
    }

    private void put(SLGearmanClient gm) throws InterruptedException {
        gms.put(gm);
        currentCount.addAndGet(1);
    }

    public void free(SLGearmanClient gmaseClient) throws InterruptedException {
        put(gmaseClient);
        //System.out.println(Thread.currentThread().getName().concat(" ")+ "free gmase connect, now have " + gms.size() + ":"+ String.valueOf(currentCount.get()) + " connect");
    }

    public void close() throws Exception {
        while (totalCount > 0) {
            for (SLGearmanClient gm : gms) {
                gm.close();
                totalCount--;
                if (totalCount < 1) {
                    break;
                }
                //System.out.println("have " + totalCount + " connect need close");
            }
        }
        //System.out.println("gmasePool close success......");
    }

    public int getCurrentCount() {
        return currentCount.get();
    }

    public int getTotalCount() {
        return totalCount;
    }
}
