package com.ifchange.spark.streaming.v2

import javax.mail.internet.InternetAddress

import com.ifchange.spark.streaming.v2.util.{JavaMailUtil, ParamParseUtil}
import kafka.serializer.StringDecoder
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * gsystem单条报警
  * update at 2018/04/02
  */
class SparkStreamingForGsystemAlarm {

}

object SparkStreamingForGsystemAlarm {
  private val logger = Logger.getLogger(classOf[SparkStreamingForGsystemAlarm])

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      logger.info("args length is not correct")
      System.exit(-1)
    }

    val receiveMails = Array[InternetAddress](
//      new InternetAddress("dongjun.xu@ifchange.com", "", "UTF-8"),
      new InternetAddress("jiqing.sun@ifchange.com", "", "UTF-8"))

    val master = args(0)
    val topic = args(1)
    val groupId = args(2)
    val appName = "spark-streaming-gsystem-alarm"
    val conf = new SparkConf()
    conf.setAppName(appName)
    conf.setMaster(master)
    conf.set("app.logging.name", "spark-streaming-gsystem-alarm")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Durations.minutes(1))
    ssc.checkpoint("/basic_data/gsystem-alarm-checks")

    val kafkaParams = Map("metadata.broker.list" -> "192.168.8.194:9092,192.168.8.195:9092,192.168.8.196:9092,192.168.8.197:9092",
      "auto.offset.reset" -> "largest",
      "zookeeper.connect" -> "192.168.8.194:2181,192.168.8.195:2181,192.168.8.196:2181,192.168.8.197:2181",
      "group.id" -> groupId)
    val topics = Set(topic)
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

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
        val w = map.getOrElse("w", "w未知")
        val c = map.getOrElse("c", "c未知")
        val m = map.getOrElse("m", "m未知")
        val hostName = map.getOrElse("hostname", "")
        val errNo = map.getOrElse("err_no", "errNo未知")
        val errMsg = map.getOrElse("err_msg", "errMsg未知")
        //发送邮件
        val subject: String = "GSYSTEM日志报警"
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
        sb.append("hostName")
        sb.append("</td>")
        sb.append("<td>")
        sb.append(hostName)
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
        sb.append("w")
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
        sb.append("err_no")
        sb.append("</td>")
        sb.append("<td>")
        sb.append(errNo)
        sb.append("</td>")
        sb.append("</tr>")

        sb.append("<tr>")
        sb.append("<td>")
        sb.append("err_msg")
        sb.append("</td>")
        sb.append("<td>")
        sb.append(errMsg)
        sb.append("</td>")
        sb.append("</table>")
        try
          JavaMailUtil.sendEmailByIfchange(subject, sb.toString, receiveMails)
        catch {
          case e: Exception => logger.info("send email error first," + e.getMessage)
        }
      }
      data
    }).window(Durations.minutes(1), Durations.minutes(1))

    //报警监控
    errorValue.foreachRDD(rdd => {
      val count = rdd.count()
      logger.info("gsystem error count:" + count)
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
