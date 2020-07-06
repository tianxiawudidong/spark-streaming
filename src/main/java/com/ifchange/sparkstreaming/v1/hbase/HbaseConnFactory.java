package com.ifchange.sparkstreaming.v1.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * hbase连接层
 *
 * @author Administrator
 */
public class HbaseConnFactory {

    private static Configuration conf = HBaseConfiguration.create();

    public static Connection createConn(String hosts, String port) {
        setConf("hbase.zookeeper.quorum", hosts);
        setConf("hbase.zookeeper.property.clientPort", port);
        setConf("zookeeper.znode.parent", "/hbase");

        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static Connection createConn() {
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static Connection createConn(String hosts) {
        return createConn(hosts, "2181");
    }

    private static void setConf(String key, String value) {
        conf.set(key, value);
    }

}
