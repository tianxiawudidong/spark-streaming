package com.ifchange.spark.streaming.v2.log

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import javax.mail.internet.InternetAddress

import com.ifchange.spark.streaming.v2.mysql.Mysql
import com.ifchange.spark.streaming.v2.util.{JavaMailUtil, ParamParseUtil}
import com.ifchange.sparkstreaming.v1.common.MysqlConfig
import com.ifchange.sparkstreaming.v1.hbase.HbasePool
import com.ifchange.sparkstreaming.v1.util.RedisCli
import kafka.serializer.StringDecoder
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import redis.clients.jedis.Jedis

/**
  * resume_parse
  * 1、流量监控   5分钟 请求总量 请求总时间
  * 2、性能监控   5分钟 f w 平均响应时间
  * 3、稳定性监控 5分钟 f w 失败率
  * 4、报警监控   1分钟
  * 5、汇总统计   5分钟汇总一次
  * redis统计
  * 错误保存hbase、mysql
  * Created by Administrator on 2018/1/11.
  * update at 5/25
  * 使用mysql 替换 mysql pool 减少连接数
  */
class SparkStreamingForResumeParseMonitorV2 {

}

object SparkStreamingForResumeParseMonitorV2 {

  private val logger = Logger.getLogger("SparkStreamingForResumeParseMonitorV2")

  private val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")

  private var redisCli = new RedisCli("192.168.8.117", 6070)

  private val RESUME_PARSE_SUCCESS = "resume_parse_success"

  private val RESUME_PARSE_FAIL = "resume_parse_fail"

  private val hbasePool = new HbasePool("resumeParse_log", "hadoop105,hadoop107,hadoop108", "2181")

  //  private val mysqlPool = new MysqlPools(MysqlConfig.USERNAME, MysqlConfig.PASSWORD, MysqlConfig.HOST, MysqlConfig.PORT, MysqlConfig.DBNAME)

  def main(args: Array[String]): Unit = {
    if (args.length < 3)
      logger.info("args length is not correct")

    val receiveMails = Array[InternetAddress](
      new InternetAddress("1484526076@qq.com", "", "UTF-8"),
      new InternetAddress("chao.jiang@ifchange.com", "", "UTF-8"),
      new InternetAddress("dongjun.xu@ifchange.com", "", "UTF-8"))

    val master = args(0)
    val topic = args(1)
    val groupId = args(2)
    val appName = "spark-streaming-resume_parse-monitor"
    val conf = new SparkConf()
    conf.setAppName(appName)
    conf.setMaster(master)
    conf.set("app.logging.name", "spark-streaming-resume_parse-monitor")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Durations.minutes(1))
    ssc.checkpoint("/algorithm/resume_parse-checkpoint")

    val kafkaParams = Map("metadata.broker.list" -> "192.168.8.194:9092,192.168.8.195:9092,192.168.8.196:9092,192.168.8.197:9092",
      "auto.offset.reset" -> "largest",
      "zookeeper.connect" -> "192.168.8.194:2181,192.168.8.195:2181,192.168.8.196:2181,192.168.8.197:2181",
      "group.id" -> groupId)
    val topics = Set(topic)
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    //    val kafkaParams = Map[String, Object](
    //      "bootstrap.servers" -> "hadoop105:9092,hadoop107:9092,hadoop108:9092",
    //      "key.deserializer" -> classOf[StringDeserializer],
    //      "value.deserializer" -> classOf[StringDeserializer],
    //      "group.id" -> groupId,
    //      "auto.offset.reset" -> "latest",
    //      "enable.auto.commit" -> (false: java.lang.Boolean)
    //    )
    //
    //    val topics = Array(topic)
    //    val lines = KafkaUtils.createDirectStream[String, String](
    //      ssc,
    //      PreferConsistent,
    //      Subscribe[String, String](topics, kafkaParams)
    //    )

    //1、流量监控
    val totalCount = lines.filter(rdd => {
      var flag = false
      val kafkaValue = rdd._2
      if (kafkaValue.contains("t=") && kafkaValue.contains("&f="))
        flag = true
      flag
    }).map(rdd => {
      val kafkaValue = rdd._2
      if (StringUtils.isNotBlank(kafkaValue))
        ("resume_parse", 1)
      else
        ("resume_parse", 0)
    }).reduceByKeyAndWindow((x: Int, y: Int) => x + y, Durations.minutes(5), Durations.minutes(5), 1)

    val totalTime = lines.filter(rdd => {
      var flag = false
      val kafkaValue = rdd._2
      if (kafkaValue.contains("t=") && kafkaValue.contains("&f="))
        flag = true
      flag
    }).map(rdd => {
      val kafkaValue = rdd._2
      var map = Map("" -> "")
      if (StringUtils.isNotBlank(kafkaValue)) {
        try {
          map = ParamParseUtil.parse(kafkaValue)
        } catch {
          case e: Exception => logger.info(kafkaValue + ",解析报错," + e.getMessage)
        }
      }
      if (map.nonEmpty) {
        var responseTime = 0.0
        try {
          responseTime = map.getOrElse("r", "0").toDouble
        } catch {
          case ex: Exception => logger.info("r转成double报错," + ex.getMessage + ",kafkaValue=" + kafkaValue)
        }
        ("resume_parse", responseTime)
      } else
        ("resume_parse", 0.0)
    }).reduceByKeyAndWindow((x: Double, y: Double) => x + y, Durations.minutes(5), Durations.minutes(5), 1)

    val pair1 = totalCount.join(totalTime)

    //2、性能统计 f w 平均响应时间(总的时间/总的请求数目)
    val totalTimeByFAndW = lines.filter(rdd => {
      var flag = false
      val kafkaValue = rdd._2
      if (kafkaValue.contains("t=") && kafkaValue.contains("&f="))
        flag = true
      flag
    }).map(rdd => {
      val kafkaValue = rdd._2
      var map = Map("" -> "")
      if (StringUtils.isNotBlank(kafkaValue)) {
        try {
          map = ParamParseUtil.parse(kafkaValue)
        } catch {
          case e: Exception => logger.info(kafkaValue + ",解析报错," + e.getMessage)
        }
      }
      if (map.nonEmpty) {
        val f = map.getOrElse("f", "f")
        val w = map.getOrElse("w", "w")
        val key = f + "+" + w
        var responseTime = 0.0
        try {
          responseTime = map.getOrElse("r", "0").toDouble
        } catch {
          case ex: Exception => logger.info("r转成double报错," + ex.getMessage + ",kafkaValue=" + kafkaValue)
        }
        (key, responseTime)
      } else
        ("f+w", 0.0)
    }).reduceByKeyAndWindow((x: Double, y: Double) => x + y, Durations.minutes(5), Durations.minutes(5), 1)

    val totalCountByFAndW = lines.filter(rdd => {
      var flag = false
      val kafkaValue = rdd._2
      if (kafkaValue.contains("t=") && kafkaValue.contains("&f="))
        flag = true
      flag
    }).map(rdd => {
      val kafkaValue = rdd._2
      var map = Map("" -> "")
      if (StringUtils.isNotBlank(kafkaValue)) {
        try {
          map = ParamParseUtil.parse(kafkaValue)
        } catch {
          case e: Exception => logger.info(kafkaValue + ",解析报错," + e.getMessage)
        }
      }
      if (map.nonEmpty) {
        val f = map.getOrElse("f", "f")
        val w = map.getOrElse("w", "w")
        val key = f + "+" + w
        (key, 1)
      } else
        ("f+w", 0)
    }).reduceByKeyAndWindow((x: Int, y: Int) => x + y, Durations.minutes(5), Durations.minutes(5), 1)

    //3、稳定性监控
    val totalFailCountByFAndW = lines.filter(rdd => {
      var flag = false
      val kafkaValue = rdd._2
      if (kafkaValue.contains("t=") && kafkaValue.contains("&f="))
        flag = true
      flag
    }).map(rdd => {
      val kafkaValue = rdd._2
      var map = Map("" -> "")
      if (StringUtils.isNotBlank(kafkaValue)) {
        try {
          map = ParamParseUtil.parse(kafkaValue)
        } catch {
          case e: Exception => logger.info(kafkaValue + ",解析报错," + e.getMessage)
        }
      }
      if (map.nonEmpty) {
        val f = map.getOrElse("f", "f")
        val w = map.getOrElse("w", "w")
        val key = f + "+" + w
        val s = map.getOrElse("s", "0")
        if (s.equals("0"))
          (key, 1.0)
        else
          (key, 0.0)
      } else
        ("f+w", 0.0)
    }).reduceByKeyAndWindow((x: Double, y: Double) => x + y, Durations.minutes(5), Durations.minutes(5), 1)

    val pair = totalFailCountByFAndW.join(totalTimeByFAndW).join(totalCountByFAndW)

    //4、报警监控
    val errorValue = lines.filter(rdd => {
      var flag = false
      val kafkaValue = rdd._2
      var map = Map("" -> "")
      if (kafkaValue.contains("t=") && kafkaValue.contains("&f=")) {
        try {
          map = ParamParseUtil.parse(kafkaValue)
        } catch {
          case e: Exception => logger.info(kafkaValue + ",解析报错," + e.getMessage)
        }
      }
      if (map.nonEmpty) {
        val s = map.getOrElse("s", "s未知")
        if (s.equals("0"))
          flag = true
      }
      flag
    }).window(Durations.minutes(1), Durations.minutes(1))

    //5、汇总统计
    val summaryValue = lines.mapPartitions(rdds => {
      val mysql = new Mysql(MysqlConfig.USERNAME, MysqlConfig.PASSWORD, MysqlConfig.DBNAME, MysqlConfig.HOST, MysqlConfig.PORT)
      var redis: Jedis = null
      try {
        redis = redisCli.getJedis
      } catch {
        case ex: Exception =>
          logger.info(ex.getMessage)
          redisCli = new RedisCli("192.168.8.117", 6070)
          redis = redisCli.getJedis
      }
      redis.auth("ruixuezhaofengnian")
      redis.select(13)
      rdds.foreach(f = value => {
        val kafkaValue = value._2
        var map = Map("" -> "")
        if (kafkaValue.contains("t=") && kafkaValue.contains("&f=")) {
          logger.info(kafkaValue)
          try {
            map = ParamParseUtil.parse(kafkaValue)
          } catch {
            case e: Exception => logger.info(kafkaValue + ",解析报错," + e.getMessage)
          }
        }
        if (map.nonEmpty) {
          val time = map.getOrElse("t", "")
          //s 是否成功
          val s: String = map.getOrElse("s", "0")
          //r 频响时间
          val r = map.getOrElse("r", "0")
          val responseTime = r.toDouble
          //w work名
          val w = map.getOrElse("w", "w未知")
          val c = map.getOrElse("c", "c未知")
          val m = map.getOrElse("m", "m未知")
          val f = map.getOrElse("f", "f未知")
          var logId = map.getOrElse("logid", "")
          if (logId.equals("null"))
            logId = ""
          val functionMark = "resumeParse_" + logId
          if (StringUtils.isNotBlank(time)) {
            val day = time.split(" ")(0)
            val successKey = RESUME_PARSE_SUCCESS + "-" + day
            val failKey = RESUME_PARSE_FAIL + "-" + day
            val workSuccessKey = w + "-success-" + day
            val workFailKey = w + "-fail-" + day
            //统计具体work+c+m
            val wcmSuccessKey = w + "-" + c + "-" + m + "-success-" + day
            val wcmFailKey = w + "-" + c + "-" + m + "-fail-" + day
            //统计wcm每次的时间[0,1)秒
            val wcmTimeKey1 = w + "-" + c + "-" + m + "-" + day + "-time1"
            //[1,5)秒
            val wcmTimeKey2 = w + "-" + c + "-" + m + "-" + day + "-time2"
            //[5,+)秒
            val wcmTimeKey3 = w + "-" + c + "-" + m + "-" + day + "-time3"
            if (!redis.exists(successKey)) redisCli.set(redis, successKey, "0")
            if (!redis.exists(failKey)) redisCli.set(redis, failKey, "0")
            if (!redis.exists(workSuccessKey)) redisCli.set(redis, workSuccessKey, "0")
            if (!redis.exists(workFailKey)) redisCli.set(redis, workFailKey, "0")
            if (!redis.exists(wcmSuccessKey)) redisCli.set(redis, wcmSuccessKey, "0")
            if (!redis.exists(wcmFailKey)) redisCli.set(redis, wcmFailKey, "0")
            if (!redis.exists(wcmTimeKey1)) redisCli.set(redis, wcmTimeKey1, "0")
            if (!redis.exists(wcmTimeKey2)) redisCli.set(redis, wcmTimeKey2, "0")
            if (!redis.exists(wcmTimeKey3)) redisCli.set(redis, wcmTimeKey3, "0")
            //redis统计次数
            if (s.equals("1")) {
              redisCli.incr(redis, successKey)
              redisCli.incr(redis, workSuccessKey)
              redisCli.incr(redis, wcmSuccessKey)
            }
            else {
              redisCli.incr(redis, failKey)
              redisCli.incr(redis, workFailKey)
              redisCli.incr(redis, wcmFailKey)
            }
            //responseTime 毫秒
            if (responseTime >= 0 && responseTime < 1000) { //[0,1)
              redisCli.incr(redis, wcmTimeKey1)
            }
            else if (responseTime >= 1000 && responseTime < 5000) { //[1,5)
              redisCli.incr(redis, wcmTimeKey2)
            }
            else { //[5,+)
              redisCli.incr(redis, wcmTimeKey3)
            }
          }
          if (s.equals("0")) {
            if (StringUtils.isNotBlank(logId)) {
              import collection.JavaConversions._
              val hbaseClient = hbasePool.getHbaseClient
              hbaseClient.setTbale("resumeParse_log")
              try {
                logger.info("save " + logId + " into hbase")
                hbaseClient.insterByMap(logId, "info", map)
                hbaseClient.saveInsert()
              } catch {
                case e: Exception => logger.info("save into hbase error," + e.getMessage)
              } finally {
                hbasePool.free(hbaseClient)
              }
            }
            val reason = "调用w:" + w + ",c:" + c + ",m:" + m + ",调用失败"
            val sql = "insert into `resume_parse_log_monitor`(`time`,`function_mark`,`is_success`,`response_time`,`work_name`,`alarm_reason`,`type`,`f`,`c`,`m`) " + "values(\"" + time + "\",\"" + functionMark + "\"," + s + "," + r + ",\"" + w + "\"," + "\"" + reason + "\"," + 0 + ",\"" + f + "\",\"" + c + "\",\"" + m + "\"" + ")"
            logger.info(sql)
            try {
              mysql.execute(sql)
            } catch {
              case e: java.sql.SQLException => logger.info("mysql execute error" + e.getMessage)
            } finally {
              mysql.close()
            }
          }
        }
      })
      redisCli.returnResource(redis)
      rdds
    }).window(Durations.minutes(5), Durations.minutes(5))

    //流量监控
    pair1.foreachRDD(rdd => {
      var count = 0L
      try {
        count = rdd.count()
      } catch {
        case e: Exception => logger.info("rdd count error," + e.getMessage)
      }
      logger.info("count:" + count)
      if (count > 0) {
        val number = count.toInt
        val list = rdd.take(number)
        val localDateTime = LocalDateTime.now()
        val before = localDateTime.minusMinutes(5L)
        val from = dtf.format(before)
        val to = dtf.format(localDateTime)
        for (array <- list) {
          val num = array._2._1
          val time = array._2._2
          val sql = "insert into `resume_parse_log_batch_result`(`from_time`,`end_time`,`total_num`,`total_use_time`,`type`)" +
            "values(\"" + from + "\",\"" + to + "\"," + num + "," + time + "," + 0 + ")"
          logger.info("sql:" + sql)
          val mysql = new Mysql(MysqlConfig.USERNAME, MysqlConfig.PASSWORD, MysqlConfig.DBNAME, MysqlConfig.HOST, MysqlConfig.PORT)
          try {
            mysql.execute(sql)
          } catch {
            case ex: java.sql.SQLException => logger.info("insert into mysql error," + ex.getMessage)
          } finally {
            mysql.close()
          }
        }
      }
    })

    //性能监控 稳定性监控
    pair.foreachRDD(rdd => {
      var count = 0L
      try {
        count = rdd.count()
      } catch {
        case e: Exception => logger.info("rdd count error," + e.getMessage)
      }
      if (count > 0) {
        val num = count.toInt
        val list: Array[(String, ((Double, Double), Int))] = rdd.take(num)
        val localDateTime = LocalDateTime.now()
        val before = localDateTime.minusMinutes(5L)
        val from = dtf.format(before)
        val to = dtf.format(localDateTime)
        val typeName = "resume_parse"
        val sql = "insert into `echeng_log_request_interval_statistics`(`f`,`work_name`,`from_time`,`end_time`,`avg_response_time`,`fail_rate`,`type_name`) values(?,?,?,?,?,?,?)"
        val mysql = new Mysql(MysqlConfig.USERNAME, MysqlConfig.PASSWORD, MysqlConfig.DBNAME, MysqlConfig.HOST, MysqlConfig.PORT)
        try {
          mysql.batchInsertForLog(sql, list, from, to, typeName)
        } catch {
          case ex: java.sql.SQLException => logger.info("insert into mysql error," + ex.getMessage)
        } finally {
          mysql.close()
        }
      }
    })

    //报警监控
    errorValue.foreachRDD(rdd => {
      rdd.foreach(value => {
        val kafkaValue = value._2
        var map = Map("" -> "")
        if (kafkaValue.contains("t=") && kafkaValue.contains("&f=")) {
          try {
            map = ParamParseUtil.parse(kafkaValue)
          } catch {
            case e: Exception => logger.info(kafkaValue + ",解析报错," + e.getMessage)
          }
        }
        if (map.nonEmpty) {
          val f = map.get("f")
          val time = map.get("t")
          val logId = map.getOrElse("logid", "logid")
          val w = map.getOrElse("w", "w")
          val c = map.getOrElse("c", "c")
          val m = map.getOrElse("m", "m")
          val msg = kafkaValue.substring(kafkaValue.indexOf("{"), kafkaValue.lastIndexOf("}"))
          if (!f.contains("spider")) {
            val subject = "RESUME_PARSE日志报警"
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
            sb.append("logid")
            sb.append("</td>")
            sb.append("<td>")
            sb.append(logId)
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
            sb.append("报警原因")
            sb.append("</td>")
            sb.append("<td>")
            sb.append(msg)
            sb.append("</td>")
            sb.append("</table>")
            try {
              JavaMailUtil.sendEmailByIfchange(subject, sb.toString, receiveMails)
            } catch {
              case e: Exception =>
                logger.info("send email by ifchange error," + e.getMessage)
            }
          }
        }
      })
    })

    //汇总统计
    summaryValue.foreachRDD(rdd => {
      val count = rdd.count()
      logger.info("count:" + count)
    })

    ssc.start()
    ssc.awaitTermination()

  }


}

