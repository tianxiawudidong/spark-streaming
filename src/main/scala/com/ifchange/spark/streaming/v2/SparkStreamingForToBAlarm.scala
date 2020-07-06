package com.ifchange.spark.streaming.v2

import javax.mail.internet.InternetAddress

import com.ifchange.spark.streaming.v2.mysql.{Mysql, MysqlPools}
import com.ifchange.spark.streaming.v2.util.{JavaMailUtil, ParamParseUtil}
import com.ifchange.sparkstreaming.v1.common.MysqlConfig
import kafka.serializer.StringDecoder
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * 1、报警监控
  * update 8/21 remove hbase
  */
class SparkStreamingForToBAlarm {

}

object SparkStreamingForToBAlarm {

  private val logger = Logger.getLogger(classOf[SparkStreamingForToBAlarm])

//  private val hbasePool = new HbasePool("tob_log", "hadoop105,hadoop107,hadoop108", "2181")

  private val mysqlPool = new MysqlPools(MysqlConfig.USERNAME, MysqlConfig.PASSWORD, MysqlConfig.HOST, MysqlConfig.PORT, MysqlConfig.DBNAME)


  def main(args: Array[String]): Unit = {
    if (args.length < 3)
      logger.error("args length is not correct")

    val receiveMails = Array[InternetAddress](
      new InternetAddress("tob@cheng95.com", "", "UTF-8"))
//      new InternetAddress("dongjun.xu@ifchange.com", "", "UTF-8"))

    val master = args(0)
    val topic = args(1)
    val groupId = args(2)
    val appName = "spark-streaming-tob-alarm"
    val conf = new SparkConf()
    conf.setMaster(master)

    conf.setAppName(appName)
    conf.set("app.logging.name", "spark-streaming-tob-alarm")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Durations.minutes(1))
    ssc.checkpoint("/basic_data/tob-alarm-checks")

    val kafkaParams = Map("metadata.broker.list" -> "192.168.8.194:9092,192.168.8.195:9092,192.168.8.196:9092,192.168.8.197:9092",
      "auto.offset.reset" -> "largest",
      "zookeeper.connect" -> "192.168.8.194:2181,192.168.8.195:2181,192.168.8.196:2181,192.168.8.197:2181",
      "group.id" -> groupId)
    val topics = Set(topic)
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)


    //1、报警监控
    val errorValue = lines.filter(rdd => {
      var flag = false
      val kafkaValue = rdd._2
      var map = Map("" -> "")
      if (StringUtils.isNotBlank(kafkaValue)) {
        try {
          map = ParamParseUtil.parse(kafkaValue)
        } catch {
          case e: Exception => logger.error(kafkaValue + ",解析报错," + e.getMessage)
        }
      }
      if (map.nonEmpty) {
        val s = map.getOrElse("s", "s未知")
        if (s.equals("0")) {
          flag = true
        }
      }
      flag
    }).map(value => {
//      val hbaseClient = hbasePool.getHbaseClient
//      hbaseClient.setTbale("tob_log")
      val data = value._2
      logger.info("-------------------")
      logger.info(data)
      logger.info("-------------------")
      var map = Map("" -> "")
      if (StringUtils.isNotBlank(data)) {
        try {
          map = ParamParseUtil.parse(data)
        } catch {
          case e: Exception => logger.error(data + ",解析报错," + e.getMessage)
        }
      }
      if (map.nonEmpty) {
        val time = map.getOrElse("t", "t未知")
        val f: String = map.getOrElse("f", "f未知")
        var logId = map.getOrElse("logid", "")
        val w = map.getOrElse("w", "w未知")
        val c = map.getOrElse("c", "c未知")
        val m = map.getOrElse("m", "m未知")
        if (StringUtils.isBlank(logId)) {
          try {
            logId = f.split("_")(1)
          } catch {
            case e: Exception => logger.info("get logid error," + e.getMessage)
          }
        }
        val functionMark = "tob_" + logId
        val s: String = map.getOrElse("s", "s未知")
        val r: String = map.getOrElse("r", "0")
        val reason = "调用w:" + w + ",c:" + c + ",m:" + m + "报错"
        //发送邮件
        val subject: String = "TOB日志报警"
        val sb = new StringBuilder
        sb.append("<h1>")
        sb.append(subject)
        sb.append("</h1>")
        sb.append("<table  border=\"1\">")
        sb.append("<tr>")
        sb.append("<td>")
        sb.append("时间")
        sb.append("</td>")
        sb.append("<td>")
        sb.append(time)
        sb.append("</td>")
        sb.append("</tr>")
        sb.append("<tr>")
        sb.append("<td>")
        sb.append("f")
        sb.append("</td>")
        sb.append("<td>")
        sb.append(f)
        sb.append("</td>")
        sb.append("</tr>")
        sb.append("<tr>")
        sb.append("<td>")
        sb.append("logId")
        sb.append("</td>")
        sb.append("<td>")
        sb.append(logId)
        sb.append("</td>")
        sb.append("</tr>")

        sb.append("<tr>")
        sb.append("<td>")
        sb.append("请求work")
        sb.append("</td>")
        sb.append("<td>")
        sb.append(w)
        sb.append("</td>")
        sb.append("</tr>")

        sb.append("<tr>")
        sb.append("<td>")
        sb.append("c")
        sb.append("</td>")
        sb.append("<td>")
        sb.append(c)
        sb.append("</td>")
        sb.append("</tr>")

        sb.append("<tr>")
        sb.append("<td>")
        sb.append("m")
        sb.append("</td>")
        sb.append("<td>")
        sb.append(m)
        sb.append("</td>")
        sb.append("</tr>")

        sb.append("<tr>")
        sb.append("<td>")
        sb.append("报警原因")
        sb.append("</td>")
        sb.append("<td>")
        sb.append(reason)
        sb.append("</td>")
        sb.append("</table>")
        try
          JavaMailUtil.sendEmailByIfchange(subject, sb.toString, receiveMails)
        catch {
          case e: Exception => logger.info("send email error first," + e.getMessage)
        }

//        import collection.JavaConversions._
//        try {
//          logger.info("save " + logId + " into hbase")
//          hbaseClient.insterByMap(logId, "info", map)
//          hbaseClient.saveInsert()
//        } catch {
//          case e: Exception => logger.info("save into hbase error," + e.getMessage)
//        } finally {
//          hbasePool.free(hbaseClient)
//        }

        val sql: String = "insert into tob_log_monitor(`time`,`function_mark`,`is_success`,`response_time`,`work_name`,`alarm_reason`,`f`,`c`,`m`) " + "values(\"" + time + "\",\"" + functionMark + "\"," + s + "," + r + ",\"" + w + "\"," + "\"" + reason + "\",\"" + f + "\",\"" + c + "\",\"" + m + "\")"
        logger.info(sql)
        var mysql: Mysql = null
        try {
          mysql = mysqlPool.getMysqlConn
        } catch {
          case ex: Exception => logger.info("get mysql from pool error," + ex.getMessage)
        } finally {
          if (null != mysql) {
            try {
              mysql.execute(sql)
            } catch {
              case e: java.sql.SQLException => logger.info("mysql execute error" + e.getMessage)
            } finally {
              mysqlPool.free(mysql)
            }
          }
        }
      }
      data
    }).window(Durations.minutes(1), Durations.minutes(1))

    //报警监控
    errorValue.foreachRDD(rdd => {
      val count = rdd.count()
      logger.info("count:" + count)
    })
    ssc.start()
    ssc.awaitTermination()

  }


}
