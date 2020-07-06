package com.ifchange.spark.streaming.v2

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import javax.mail.internet.InternetAddress

import com.ifchange.spark.streaming.v2.mysql.{Mysql, MysqlPools}
import com.ifchange.spark.streaming.v2.util.{JavaMailUtil, ParamParseUtil}
import com.ifchange.sparkstreaming.v1.common.MysqlConfig
import com.ifchange.sparkstreaming.v1.util.RedisCli
import kafka.serializer.StringDecoder
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * RELATION
  * 1、流量监控   5分钟请求量、请求总时间
  * 2、性能监控   5分钟 f w 以及平均响应时间
  * 3、稳定性监控 5分钟 f w 失败率
  * 4、报警监控   5分钟超时50次 报警
  * 5、汇总统计   5分钟汇总一次
  * redis统计
  * 错误、超时保存 hbase mysql
  * update 8/21 remove hbase
  * Created by Administrator on 2018/1/10.
  */
class SparkStreamingForRelationMonitor {

}

object SparkStreamingForRelationMonitor {

  private val logger = Logger.getLogger(classOf[SparkStreamingForRelationMonitor])

  private val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")

  private var redisCli = new RedisCli("192.168.8.117", 6070)

  private val RELATION_SUCCESS = "relation_success"

  private val RELATION_FAIL = "relation_fail"

//  private val hbasePool = new HbasePool("relation_log", "hadoop105,hadoop107,hadoop108", "2181")

  private val mysqlPool = new MysqlPools(MysqlConfig.USERNAME, MysqlConfig.PASSWORD, MysqlConfig.HOST, MysqlConfig.PORT, MysqlConfig.DBNAME)

  def main(args: Array[String]): Unit = {
    if (args.length < 3)
      logger.info("args length is not correct")

    val receiveMails = Array[InternetAddress](
//      new InternetAddress("dongjun.xu@ifchange.com", "", "UTF-8"),
      new InternetAddress("xuelin.zhou@ifchange.com", "", "UTF-8"))

    val master = args(0)
    val topic = args(1)
    val groupId = args(2)
    val appName = "spark-streaming-relation-monitor"
    val conf = new SparkConf()
    conf.setAppName(appName)
    conf.setMaster(master)
    conf.set("app.logging.name", "spark-streaming-relation-monitor")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Durations.minutes(1))
    ssc.checkpoint("/basic_data/relation-checks")

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
    val totalCount = lines.map(rdd => {
      val kafkaValue = rdd._2
      if (StringUtils.isNotBlank(kafkaValue)) {
        ("relation", 1)
      } else
        ("relation", 0)
    }).reduceByKeyAndWindow((a: Int, b: Int) => a + b, Durations.minutes(5), Durations.minutes(5), 1)

    val totalTime = lines.map(rdd => {
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
        ("relation", responseTime)
      } else
        ("relation", 0.0)
    }).reduceByKeyAndWindow((a: Double, b: Double) => a + b, Durations.minutes(5), Durations.minutes(5), 1)

    val pair1 = totalCount.join(totalTime)

    //2、性能监控
    val totalTimeByFAndW = lines.map(rdd => {
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
        val w = map.getOrElse("worker_name", "w")
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
    }).reduceByKeyAndWindow((a: Double, b: Double) => a + b, Durations.minutes(5), Durations.minutes(5), 1)

    val totalCountByFAndW = lines.map(rdd => {
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
        val w = map.getOrElse("worker_name", "w")
        val key = f + "+" + w
        (key, 1)
      } else
        ("f+w", 0)
    }).reduceByKeyAndWindow((a: Int, b: Int) => a + b, Durations.minutes(5), Durations.minutes(5), 1)

    //3、稳定性监控
    val totalFailCountByFAndW = lines.map(rdd => {
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
        val w = map.getOrElse("worker_name", "w")
        val key = f + "+" + w
        val s = map.getOrElse("s", "0")
        if (s.equals("0"))
          (key, 1.0)
        else
          (key, 0.0)
      } else
        ("f+w", 0.0)
    }).reduceByKeyAndWindow((a: Double, b: Double) => a + b, Durations.minutes(5), Durations.minutes(5), 1)

    val pair = totalFailCountByFAndW.join(totalTimeByFAndW).join(totalCountByFAndW)

    //4、报警监控
    val monitorValue = lines.map(rdd => {
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
        val w = map.getOrElse("worker_name", "w")
        var responseTime = 0.0
        try {
          responseTime = map.getOrElse("r", "0").toDouble
        } catch {
          case ex: Exception => logger.info("r转成double报错," + ex.getMessage + ",kafkaValue=" + kafkaValue)
        }
        if (responseTime >= 5000)
          (w, 1)
        else
          (w, 0)
      } else
        ("w", 0)
    }).reduceByKeyAndWindow((a: Int, b: Int) => a + b, Durations.minutes(5), Durations.minutes(5), 1)

    //汇总统计
    val summaryValue = lines.mapPartitions(rdds => {
      //redis保存
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
      redis.select(1)
      rdds.foreach(value => {
        val kafkaValue = value._2
        var map = Map("" -> "")
        if (StringUtils.isNotBlank(kafkaValue)) {
          try {
            map = ParamParseUtil.parse(kafkaValue)
          } catch {
            case e: Exception => logger.info(kafkaValue + ",解析报错," + e.getMessage)
          }
        }
        if (map.nonEmpty) {
          val time = map.getOrElse("t", "")
          //s 是否成功
          val s = map.getOrElse("s", "0")
          //r 频响时间
          val r = map.getOrElse("r", "0")
          val responseTime = r.toInt
          //w work名
          val w = map.getOrElse("worker_name", "w未知")
          val c = map.getOrElse("c", "c未知")
          val m = map.getOrElse("m", "m未知")
          val productName = map.getOrElse("product_name", "product_name")
          var compareNumber = 0
          if (productName.equals("recommend"))
            compareNumber = 2000
          else
            compareNumber = 500
          val logId = map.getOrElse("log_id", "log_id未知")
          //f 服务标记
          val f = map.getOrElse("f", "f未知")
          //w work名
          val errNo = map.getOrElse("err_no", "未知")
          val errMsg = map.getOrElse("err_msg", "未知")
          var reason = ""
          val functionMark = f + "_" + logId
          if (StringUtils.isNotBlank(time)) {
            val day = time.split(" ")(0)
            val successKey = RELATION_SUCCESS + "-" + day
            val failKey = RELATION_FAIL + "-" + day
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
            s match {
              case "1" =>
                //每天relation总的成功次数
                redisCli.incr(redis, successKey)
                //每天relation work成功的次数
                redisCli.incr(redis, workSuccessKey)
                //每天relation work+c+m成功的次数
                redisCli.incr(redis, wcmSuccessKey)
              case "0" =>
                //每天relation总的失败次数
                redisCli.incr(redis, failKey)
                //每天relation work失败的次数
                redisCli.incr(redis, workFailKey)
                //每天relation  work+c+m失败的次数
                redisCli.incr(redis, wcmFailKey)
              case _ =>
                logger.info("s没有解析出来:" + value)
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
          if (s.equals("0") || responseTime >= compareNumber) {
            if (s == "0")
              reason = "调用w:" + w + ",c:" + c + ",m:" + m + ",调用失败" + "，errNo:" + errNo + ",errMsg:" + errMsg
            if (responseTime >= compareNumber)
              reason = "调用w:" + w + ",c:" + c + ",m:" + m + ",频响时间超过" + compareNumber + "毫秒"
            //将错误信息保存到hbase和mysql
//            import collection.JavaConversions._
//            val hbaseClient = hbasePool.getHbaseClient
//            hbaseClient.setTbale("relation_log")
//            try {
//              hbaseClient.insterByMap(logId, "info", map)
//              hbaseClient.saveInsert()
//            } catch {
//              case e: Exception => logger.info("save " + logId + " into hbase error," + e.getMessage)
//            } finally {
//              hbasePool.free(hbaseClient)
//            }
            val sql = "insert into log_monitor(`time`,`function_mark`,`is_success`,`response_time`,`work_name`,`alarm_reason`,`type`,`f`,`c`,`m`) " + "values(\"" + time + "\",\"" + functionMark + "\"," + s + "," + r + ",\"" + w + "\"," + "\"" + reason + "\"," + 1 + ",\"" + f + "\",\"" + c + "\",\"" + m + "\"" + ")"
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
          val sql = "insert into `log_batch_result`(`from_time`,`end_time`,`total_num`,`total_use_time`,`type`)" +
            "values(\"" + from + "\",\"" + to + "\"," + num + "," + time + "," + 1 + ")"
          logger.info("sql:" + sql)
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
                case ex: java.sql.SQLException => logger.info("insert into mysql error," + ex.getMessage)
              } finally {
                mysqlPool.free(mysql)
              }
            }
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
        val typeName = "relation"
        val sql = "insert into `echeng_log_request_interval_statistics`(`f`,`work_name`,`from_time`,`end_time`,`avg_response_time`,`fail_rate`,`type_name`) values(?,?,?,?,?,?,?)"
        var mysql: Mysql = null
        try {
          mysql = mysqlPool.getMysqlConn
        } catch {
          case ex: Exception => logger.info("get mysql from pool error," + ex.getMessage)
        } finally {
          if (null != mysql) {
            try {
              mysql.batchInsertForLog(sql, list, from, to, typeName)
            } catch {
              case ex: java.sql.SQLException => logger.info("insert into mysql error," + ex.getMessage)
            } finally {
              mysqlPool.free(mysql)
            }
          }
        }
      }
    })

    //报警监控
    monitorValue.foreachRDD(rdd => {
      var count = 0L
      try {
        count = rdd.count()
      } catch {
        case e: Exception => logger.info("rdd count error" + e.getMessage)
      }
      if (count > 0) {
        val num = count.toInt
        val list: Array[(String, Int)] = rdd.take(num)
        for (data <- list) {
          val w = data._1
          val num = data._2
          val localDateTime = LocalDateTime.now()
          val before = localDateTime.minusMinutes(5L)
          val from = dtf.format(before)
          val to = dtf.format(localDateTime)
          if (num >= 50) {
            val reason = from + "到" + to + ",超时次数:" + num
            //发送邮件
            val subject = "RELATION日志报警"
            val sb = new StringBuilder
            sb.append("<h1>")
            sb.append(subject)
            sb.append("</h1>")
            sb.append("<table  border=\"1\">")
            sb.append("<tr>")
            sb.append("<td>")
            sb.append("work_name")
            sb.append("</td>")
            sb.append("<td>")
            sb.append(w)
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
          }
        }
      }
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
