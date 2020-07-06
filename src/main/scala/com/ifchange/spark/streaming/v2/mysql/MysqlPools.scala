package com.ifchange.spark.streaming.v2.mysql

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger


class MysqlPools extends Serializable {
  private val initialConnections = 8 // 连接池的初始大小

  private var incrementalConnections = 2 // 连接池自动增加的大小

  private var maxConnections = 20 // 连接池最大的大小

  private var mysql = new LinkedBlockingQueue[Mysql](maxConnections) // 存放连接池中数据库连接的向量

  private var totalCount: Int = 0 //总共连接数

  private var currentCount = new AtomicInteger(0) //当前可用连接数

  private var dbHost = "127.0.0.1"
  private var dbPort = 3306
  private var tableName = "mysql"
  private var dbUsername = ""
  private var passWord = ""

  def this(username: String, password: String, host: String, port: Int, dbName: String) {
    this()
    tableName = dbName
    dbUsername = username
    dbHost = host
    dbPort = port
    passWord = password
    initConn()
  }

  def this(username: String, password: String, host: String, port: Int) {
    this()
    dbUsername = username
    dbHost = host
    dbPort = port
    passWord = password
    initConn()
  }

  def this(username: String, password: String, host: String) {
    this()
    dbUsername = username
    dbHost = host
    passWord = password
    initConn()
  }

  def this(username: String, password: String) {
    this()
    dbUsername = username
    passWord = password
    initConn()
  }

  def setMaxNumber(number: Int): Unit = {
    if (number <= initialConnections) return
    maxConnections = number
    mysql = new LinkedBlockingQueue[Mysql](maxConnections)
    currentCount = new AtomicInteger(0)
    totalCount = 0
    try
      initConn()
    catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
  }

  def setAutoIncreaseNumber(number: Int): Unit = {
    incrementalConnections = number
  }

  def getMysqlConn: Mysql = {
    if (currentCount.get < 1) addConn()
    val mysql = take
    mysql
  }

  @throws[InterruptedException]
  @throws[Exception]
  private def addConn(): Unit = {
    var i = 0
    while (i < incrementalConnections && totalCount < maxConnections) {
      mysql.put(createMysqlConn)
      currentCount.addAndGet(1)
      totalCount = totalCount + 1
      i = i + 1
    }
  }

  @throws[InterruptedException]
  @throws[Exception]
  private def initConn(): Unit = {
    var i: Int = 0
    while (i < initialConnections) {
      mysql.put(createMysqlConn)
      currentCount.addAndGet(1)
      totalCount = totalCount + 1
      i = i + 1
    }
  }

  @throws[Exception]
  private def createMysqlConn = {
    val mysql = new Mysql(dbUsername, passWord, tableName, dbHost, dbPort)
    mysql
  }

  @throws[InterruptedException]
  private def take = {
    currentCount.decrementAndGet
    val db = mysql.take
    db.busy()
    db
  }

  @throws[InterruptedException]
  private def put(db: Mysql): Unit = {
    db.free()
    mysql.put(db)
    currentCount.addAndGet(1)
  }

  @throws[InterruptedException]
  def free(mysql: Mysql): Unit = {
    put(mysql)
  }

  @throws[Exception]
  def close(): Unit = {
    while (totalCount > 0) {
      import scala.collection.JavaConversions._
      for (db <- mysql) {
        db.close()
        totalCount -= 1
        if (totalCount < 1)
          System.out.println("have " + totalCount + " connect need close")
      }
    }
  }

  def getHost: String = dbHost.concat(":").concat(String.valueOf(dbPort))

  def getCurrentCount: Int = currentCount.get

  def getTotalCount: Int = totalCount

}
