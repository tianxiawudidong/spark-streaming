package com.ifchange.sparkstreaming.v1.mysql;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * 数据库连接层MYSQL
 *
 * @author Administrator
 *
 */
public class DBConnection {

    public static Connection getDBConnection(String username, String password, String dbname, String host, int port, String encoding) {
        // 1. 注册驱动
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        // 获取数据库的连接
        try {
            String conn_url = "jdbc:mysql://" + host + ":" + port + "/" + dbname + "?useUnicode=true&characterEncoding=" + encoding+"&zeroDateTimeBehavior=convertToNull";
            Connection conn = java.sql.DriverManager.getConnection(conn_url, username, password);
            return conn;
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
        return null;
    }

    public static Connection getDBConnection(String username, String password, String dbname, String host, int port) {
        return DBConnection.getDBConnection(username, password, dbname, host, port, "utf-8");
    }

    public static Connection getDBConnection(String username, String password, String dbname, String host) {
        return DBConnection.getDBConnection(username, password, dbname, host, 3306, "utf-8");
    }

    public static Connection getDBConnection(String username, String password, String dbname) {
        return DBConnection.getDBConnection(username, password, dbname, "127.0.0.1", 3306, "utf-8");
    }

    public static Connection getDBConnection(String username, String password) {
        return DBConnection.getDBConnection(username, password, "mysql", "127.0.0.1", 3306, "utf-8");
    }
}
