package com.ifchange.spark.streaming.v2.mysql

import java.io.IOException
import java.sql.{Connection, PreparedStatement, ResultSet, SQLException, Statement, Types}
import java.util

import com.ifchange.sparkstreaming.v1.mysql.DataTable
import com.ifchange.sparkstreaming.v1.util.MyString
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.streaming.kafka.OffsetRange

class Mysql extends Serializable {
  private var pstmt: PreparedStatement = _
  private var conn: Connection = _
  private var rs: ResultSet = _
  private var isBusy = false
  private var tableName = "mysql"
  private var dbUsername = ""
  private var passWord = ""
  private var dbhost = "127.0.0.1"
  private var dbport = 3306
  private var enCoding = "utf-8"
  private val logger = Logger.getLogger(classOf[Mysql])

  def this(username: String, password: String) {
    this()
    dbUsername = username
    passWord = password
    createConn()
  }

  def this(username: String, password: String, dbname: String) {
    this()
    tableName = dbname
    dbUsername = username
    passWord = password
    createConn()
  }

  def this(username: String, password: String, dbname: String, host: String) {
    this()
    tableName = dbname
    dbUsername = username
    dbhost = host
    passWord = password
    createConn()
  }

  def this(username: String, password: String, dbname: String, host: String, port: Int) {
    this()
    tableName = dbname
    dbUsername = username
    dbhost = host
    dbport = port
    passWord = password
    createConn()
  }

  def this(username: String, password: String, dbname: String, host: String, port: Int, encoding: String) {
    this()
    tableName = dbname
    dbUsername = username
    dbhost = host
    dbport = port
    passWord = password
    enCoding = encoding
    createConn()
  }

  def getTableName: String = tableName

  def getHost: String = dbhost

  private def createConn(): Unit = {
    conn = DBConnections.getDBConnection(dbUsername, passWord, tableName, dbhost, dbport, enCoding)
  }

  @throws[SQLException]
  def updateOrAdd(coulmn: Array[String], `type`: Array[Int], sql: String): Boolean = {
    if (!setPstmtParam(coulmn, `type`, sql)) return false
    val flag = if (pstmt.executeUpdate > 0) true
    else false
    close()
    flag
  }

  @throws[SQLException]
  def getResultData(coulmn: Array[String], `type`: Array[Int], sql: String): DataTable = {
    val dt = new DataTable
    val list = new util.ArrayList[util.HashMap[String, String]]
    if (!setPstmtParam(coulmn, `type`, sql)) return null
    rs = pstmt.executeQuery
    val rsmd = rs.getMetaData
    //取数据库的列名
    val numberOfColumns = rsmd.getColumnCount
    while (rs.next) {
      val rsTree = new util.HashMap[String, String]
      var r = 1
      while (r < numberOfColumns + 1) {
        rsTree.put(rsmd.getColumnName(r), rs.getObject(r).toString)
        r = r + 1
      }
      list.add(rsTree)
    }
    close()
    dt.setDataTable(list)
    dt
  }

  @throws[NumberFormatException]
  @throws[SQLException]
  private def setPstmtParam(coulmn: Array[String], `type`: Array[Int], sql: String): Boolean = {
    if (sql == null) return false
    pstmt = conn.prepareStatement(sql)
    if (coulmn != null && `type` != null && coulmn.length != 0 && `type`.length != 0) {
      var i = 0
      while (i < `type`.length) {
        `type`(i) match {
          case Types.INTEGER =>
            pstmt.setInt(i + 1, coulmn(i).toInt)
          case Types.SMALLINT =>
            pstmt.setInt(i + 1, coulmn(i).toInt)
          case Types.BOOLEAN =>
            pstmt.setBoolean(i + 1, coulmn(i).toBoolean)
          case Types.CHAR =>
            pstmt.setString(i + 1, coulmn(i))
          case Types.DOUBLE =>
            pstmt.setDouble(i + 1, coulmn(i).toDouble)
          case Types.FLOAT =>
            pstmt.setFloat(i + 1, coulmn(i).toFloat)
          case Types.BIGINT =>
            pstmt.setLong(i + 1, coulmn(i).toLong)
          case _ =>
        }
        i = i + 1
      }
    }
    true
  }

  @throws[SQLException]
  def close(): Unit = {
    if (rs != null) {
      rs.close()
      rs = null
    }
    if (pstmt != null) {
      pstmt.close()
      pstmt = null
    }
    if (conn != null) conn.close()
  }

  @throws[SQLException]
  def executeQueryCompress(sql: String): String = {
    busy()
    var compressStr = ""
    try {
      pstmt = conn.prepareStatement(sql)
      rs = pstmt.executeQuery
      while (rs.next) {
        /**
          * 8/23
          * 数据库resume_extras compress字段改成16进制字符串
          */
        val compress = rs.getString("compress")
        if (StringUtils.isNotBlank(compress))
          try {
            compressStr = MyString.unzipString(MyString.hexStringToBytes(compress))
          } catch {
            case _: IOException =>
              val compress2 = rs.getBlob("compress")
              compressStr = MyString.unzipString(compress2.getBytes(1, compress2.length.toInt))
          }
      }
      free()
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
    compressStr
  }

  @throws[Exception]
  def queryCompress(sql: String): String = {
    busy()
    var compressStr = ""
    pstmt = conn.prepareStatement(sql)
    rs = pstmt.executeQuery
    try {
      val compress = rs.getString("compress")
      if (StringUtils.isNotBlank(compress)) {
        val bytes = MyString.hexStringToBytes(compress)
        compressStr = MyString.unzipString(bytes)
      }
    } catch {
      case _: Exception =>
        val compress = rs.getBlob("compress")
        if (null != compress) compressStr = MyString.unzipString(compress.getBytes(1, compress.length.toInt))
    }
    free()
    compressStr
  }

  @throws[SQLException]
  def executeQuerysCompress(sql: String): util.List[util.Map[String, String]] = {
    busy()
    val list = new util.ArrayList[util.Map[String, String]]
    try {
      pstmt = conn.prepareStatement(sql)
      rs = pstmt.executeQuery
      while (rs.next) {
        val map = new util.HashMap[String, String]
        val id = rs.getLong("id")
        map.put("id", String.valueOf(id))
        /**
          * 8/23
          * 数据库resume_extras compress字段16进制字符串
          */
        val compress = rs.getString("compress")
        //Blob compress = rs.getBlob("compress");
        val compressStr = MyString.unzipString(MyString.hexStringToBytes(compress))
        map.put("compress", compressStr)
        list.add(map)
      }
      free()
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
    list
  }


  @throws[SQLException]
  def executeQueryOffset(sql: String): util.List[util.Map[String, String]] = {
    busy()
    val list = new util.ArrayList[util.Map[String, String]]
    try {
      pstmt = conn.prepareStatement(sql)
      rs = pstmt.executeQuery
      while (rs.next) {
        val map = new util.HashMap[String, String]
        val id = rs.getLong("id")
        map.put("id", id.toString)
        val partition = rs.getString("partition")
        map.put("partition", partition)
        val offset = rs.getLong("offset")
        map.put("offset", offset.toString)
        list.add(map)
      }
      free()
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
    list
  }


  @throws[SQLException]
  def executeUpdate(sql: String): Int = {
    busy()
    var result = 0
    try {
      pstmt = conn.prepareStatement(sql)
      result = pstmt.executeUpdate
      free()
    } catch {
      case ex: SQLException => processException(ex)
    }
    result
  }

  @throws[SQLException]
  def executeInsert(sql: String): Int = {
    busy()
    var id = 0
    try {
      pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
      pstmt.executeUpdate
      val res = pstmt.getGeneratedKeys
      while (res.next)
        id = res.getInt(1)
      free()
    } catch {
      case ex: SQLException =>
        processException(ex)
    }
    id
  }

  @throws[SQLException]
  def batchInsertForLog(sql: String, list: Array[(String, ((Double, Double), Int))], from: String, to: String, typeName: String): Unit = {
    busy()
    conn.setAutoCommit(false)
    pstmt = conn.prepareStatement(sql)
    for (data <- list) {
      val key = data._1
      val failCount = data._2._1._1
      val totalTime = data._2._1._2
      val totalNum = data._2._2
      val split = key.split("\\+")
      val f = split(0)
      val w = split(1)
      val avgTime = totalTime / totalNum
      val failRate = failCount / totalNum
      val str = "f=%1$s,w=%2$s,from=%3$s,to=%4$s,avgTime=%5$f,failRate=%6$f,typeName=%7$s".format(f, w, from, to, avgTime, failRate, typeName)
      logger.info(str)
      pstmt.setString(1, f)
      pstmt.setString(2, w)
      pstmt.setString(3, from)
      pstmt.setString(4, to)
      pstmt.setDouble(5, avgTime)
      pstmt.setDouble(6, failRate)
      pstmt.setString(7, typeName)
      pstmt.addBatch()
    }
    pstmt.executeBatch()
    conn.commit()
    free()
  }


  //update `offset_message` set `offset`=? where topic=? and `partition`=?
  @throws[SQLException]
  def batchInsertForOffset(sql: String, range: Array[OffsetRange], topic: String): Unit = {
    busy()
    conn.setAutoCommit(false)
    pstmt = conn.prepareStatement(sql)
    for (data <- range) {
      val partition = data.partition.toString
      val offset = data.fromOffset
      pstmt.setLong(1, offset)
      pstmt.setString(2, topic)
      pstmt.setString(3, partition)
      pstmt.addBatch()
    }
    pstmt.executeBatch()
    conn.commit()
    free()
  }

  //insert into `echeng_log_request_interval_statistics`(`f`,`work_name`,`from_time`,`end_time`,`avg_response_time`,`type_name`)
  // values(?,?,?,?,?,?)
  @throws[SQLException]
  def batchInsertForT0b1(sql: String, list: Array[(String, (Double, Int))], from: String, to: String, typeName: String): Unit = {
    busy()
    conn.setAutoCommit(false)
    pstmt = conn.prepareStatement(sql)
    for (data <- list) {
      val key = data._1
      val time = data._2._1
      val count = data._2._2
      val split = key.split("\\+")
      val f = split(0)
      val w = split(1)
      val avgTime = time / count
      val str = "f=%1$s,w=%2$s,from=%3$s,to=%4$s,avgTime=%5$f,typeName=%6$s".format(f, w, from, to, avgTime, typeName)
      logger.info(str)
      pstmt.setString(1, f)
      pstmt.setString(2, w)
      pstmt.setString(3, from)
      pstmt.setString(4, to)
      pstmt.setDouble(5, avgTime)
      pstmt.setString(6, typeName)
      pstmt.addBatch()
    }
    pstmt.executeBatch()
    conn.commit()
    free()
  }

  //insert into `echeng_log_request_interval_statistics`(`work_name`,`from_time`,`end_time`,`avg_response_time`,`fail_rate`,`type_name`)
  // values(?,?,?,?,?,?)
  @throws[SQLException]
  def batchInsertForTob2(sql: String, list: Array[(String, ((Double, Double), Int))], from: String, to: String, typeName: String): Unit = {
    busy()
    conn.setAutoCommit(false)
    pstmt = conn.prepareStatement(sql)
    for (data <- list) {
      val w = data._1
      val failCount = data._2._1._1
      val totalTime = data._2._1._2
      val totalNum = data._2._2
      val avgTime = totalTime / totalNum
      val failRate = failCount / totalNum
      val str = "w=%1$s,from=%2$s,to=%3$s,avgTime=%4$f,failRate=%5$f,typeName=%6$s".format(w, from, to, avgTime, failRate, typeName)
      logger.info(str)
      pstmt.setString(1, w)
      pstmt.setString(2, from)
      pstmt.setString(3, to)
      pstmt.setDouble(4, avgTime)
      pstmt.setDouble(5, failRate)
      pstmt.setString(6, typeName)
      pstmt.addBatch()
    }
    pstmt.executeBatch()
    conn.commit()
    free()
  }


  @throws[SQLException]
  def execute(sql: String): Boolean = {
    busy()
    var result = false
    pstmt = conn.prepareStatement(sql)
    result = pstmt.execute(sql)
    free()
    result
  }

  @throws[SQLException]
  def listTable: util.List[String] = {
    busy()
    val tables = new util.ArrayList[String]
    try {
      pstmt = conn.prepareStatement(String.format("SHOW TABLES FROM `%s`", tableName))
      rs = pstmt.executeQuery
      while (rs.next)
        tables.add(rs.getObject("Tables_in_".concat(tableName)).toString)
      free()
    } catch {
      case ex: SQLException => processException(ex)
    }
    tables
  }

  @throws[SQLException]
  def listDatabase: util.List[String] = {
    busy()
    val databases = new util.ArrayList[String]
    try {
      pstmt = conn.prepareStatement("show databases")
      rs = pstmt.executeQuery
      while (rs.next)
        databases.add(rs.getObject("Database").toString)
      free()
    } catch {
      case ex: SQLException => processException(ex)
    }
    databases
  }

  def busy(): Unit = {
    isBusy = true
  }

  @throws[SQLException]
  def free(): Unit = {
    isBusy = false
    if (pstmt != null) {
      pstmt.close()
      pstmt = null
    }
    if (rs != null) {
      rs.close()
      rs = null
    }
  }

  def isbusy: Boolean = isBusy

  @throws[SQLException]
  def setTableName(table: String): Unit = {
    tableName = table
    execute("use `" + table + "`")
  }

  def isValid: Boolean = try
    conn.isValid(3000)
  catch {
    case e: SQLException => e.printStackTrace()
      false
  }

  @throws[SQLException]
  def isSupportsBatchUpdates: Boolean = { //conn是Connection的类型
    val dbmd = conn.getMetaData
    //为true则意味着该数据是支持批量更新的
    dbmd.supportsBatchUpdates
  }

  def getConn: Connection = {
    reConnect()
    conn
  }

  private def reConnect(): Unit = {
    while ( {
      true
    }) try {
      free()
      if (!conn.isValid(3000)) {
        close()
        createConn()
      }
    } catch {
      case ex: SQLException =>
        ex.printStackTrace()
        try
          Thread.sleep(1000)
        catch {
          case e: InterruptedException => e.printStackTrace()

        }
    }
  }

  @throws[SQLException]
  private def processException(ex: SQLException) = {
    val err_code = ex.getErrorCode
    val err_msg = ex.getMessage
    System.out.println("_._._._._._._._._._" + err_code + ":" + err_msg)
    if (err_code == 2013 || err_code == 2006 || err_code == 1053 || err_msg.indexOf("No operations allowed after connection closed") >= 0 || err_msg.indexOf("The last packet successfully received from") >= 0) {
      ex.printStackTrace()
      reConnect()
    }
    else throw new SQLException(ex.getMessage, ex.getSQLState, err_code)
    true
  }
}
