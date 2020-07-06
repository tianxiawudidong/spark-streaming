package com.ifchange.spark.streaming.v2.mysql

import java.sql.Connection

import com.ifchange.sparkstreaming.v1.mysql.DBConnection

class DBConnections {

}

object DBConnections {
  def getDBConnection(username: String, password: String, dbname: String, host: String, port: Int, encoding: String): Connection = { // 1. 注册驱动
    try
      Class.forName("com.mysql.jdbc.Driver")
    catch {
      case e: ClassNotFoundException =>
        e.printStackTrace()
    }
    // 获取数据库的连接
    val conn_url = "jdbc:mysql://" + host + ":" + port + "/" + dbname + "?autoReconnect=true&useUnicode=true&characterEncoding=" + encoding + "&zeroDateTimeBehavior=convertToNull"
    val connection = java.sql.DriverManager.getConnection(conn_url, username, password)
    connection
  }

  def getDBConnection(username: String, password: String, dbname: String, host: String, port: Int): Connection = DBConnection.getDBConnection(username, password, dbname, host, port, "utf-8")

  def getDBConnection(username: String, password: String, dbname: String, host: String): Connection = DBConnection.getDBConnection(username, password, dbname, host, 3306, "utf-8")

  def getDBConnection(username: String, password: String, dbname: String): Connection = DBConnection.getDBConnection(username, password, dbname, "127.0.0.1", 3306, "utf-8")

  def getDBConnection(username: String, password: String): Connection = DBConnection.getDBConnection(username, password, "mysql", "127.0.0.1", 3306, "utf-8")


}
