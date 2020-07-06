package com.ifchange.sparkstreaming.v1.mysql;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;


/**
 *
 * @author root
 */
public class MysqlPool implements Serializable{

    private int initialConnections =10; // 连接池的初始大小
    private int incrementalConnections = 4;// 连接池自动增加的大小
    private int maxConnections = 128; // 连接池最大的大小
    private BlockingQueue<Mysql> mysqls = new LinkedBlockingQueue<>(maxConnections); // 存放连接池中数据库连接的向量
    private int totalCount = 0;//总共连接数
    private AtomicInteger currentCount = new AtomicInteger(0);//当前可用连接数
    private String dbhost = "127.0.0.1";
    private int dbport = 3306;
    private String dbtableName = "mysql";
    private String dbUsername;
    private String passWord;

    public MysqlPool(String username, String password, String host, int port, String dbName) throws  Exception {
        dbtableName = dbName;
        dbUsername = username;
        dbhost = host;
        dbport = port;
        passWord = password;
        initConn();
    }

    public MysqlPool(String username, String password, String host, int port) throws  Exception {
        dbUsername = username;
        dbhost = host;
        dbport = port;
        passWord = password;
        initConn();
    }

    public MysqlPool(String username, String password, String host) throws  Exception {
        dbUsername = username;
        dbhost = host;
        passWord = password;
        initConn();
    }

    public MysqlPool(String username, String password) throws  Exception {
        dbUsername = username;
        passWord = password;
        initConn();
    }

    public void setMaxNumber(int number) {
        if (number <= initialConnections) {
            return;
        }
        maxConnections = number;
        mysqls = new LinkedBlockingQueue<>(maxConnections);
        currentCount = new AtomicInteger(0);
        totalCount = 0;
        try {
            initConn();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void setAutoIncreaseNumber(int number) {
        incrementalConnections = number;
    }

    public Mysql getMysqlConn() throws  Exception {
        if (currentCount.get() < 1) {
            addHbaseConn();
        }
        return take();
    }

    private synchronized void addHbaseConn() throws Exception {
        //System.out.println(Thread.currentThread().getName().concat(" ")+ "dbase connect not enough, create it");
        for (int i = 0; i < incrementalConnections && totalCount < maxConnections; i++) {
            mysqls.put(createMysqlConn());
            currentCount.addAndGet(1);
            totalCount++;
        }
    }

    private void initConn() throws Exception {
        for (int i = 0; i < initialConnections; i++) {
            mysqls.put(createMysqlConn());
            currentCount.addAndGet(1);
            totalCount++;
        }
    }

    private Mysql createMysqlConn() throws Exception {
        return new Mysql(dbUsername, passWord, dbtableName, dbhost, dbport);
    }

    private Mysql take() throws InterruptedException {
        currentCount.decrementAndGet();
        Mysql db = mysqls.take();
        db.busy();
        return db;
    }

    private void put(Mysql db) throws InterruptedException, SQLException {
        db.free();
        mysqls.put(db);
        currentCount.addAndGet(1);
    }

    public void free(Mysql dbaseClient) throws InterruptedException, SQLException {
        put(dbaseClient);
        //System.out.println(Thread.currentThread().getName().concat(" ")+ "free dbase connect, now have " + mysqls.size() + ":"+ String.valueOf(currentCount.get()) + " connect");
    }

    public void close() throws Exception {
        while (totalCount > 0) {
            for (Mysql db : mysqls) {
                db.close();
                totalCount--;
                if (totalCount < 1) break;
            }
        }
    }

    public String getHost() {
        return dbhost.concat(":").concat(String.valueOf(dbport));
    }

    public int getCurrentCount() {
        return currentCount.get();
    }

    public int getTotalCount() {
        return totalCount;
    }
}
