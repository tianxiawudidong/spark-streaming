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
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import redis.clients.jedis.Jedis

/**
  * position
  * 1、流量监控   5分钟 请求总量、请求总时间
  * 2、性能监控   5分钟 f w 平均响应时间
  * 3、稳定性监控 5分钟 f w 失败率
  * 4、报警监控   5分钟 失败 50次报警
  * 5、汇总统计   5分钟汇总一次
  * redis统计
  * 错误保存hbase mysql
  * Created by Administrator on 2018/1/11.
  * update at 5/25
  * 使用mysql 替换 mysql pool 减少连接数
  */
class SparkStreamingForPositionMonitorV2 {

}

object SparkStreamingForPositionMonitorV2 {
  private val logger = Logger.getLogger(classOf[SparkStreamingForPositionMonitorV2])

  private val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")

  private var redisCli = new RedisCli("192.168.8.117", 6070)

  private val POSITION_SUCCESS = "position_success"

  private val POSITION_FAIL = "position_fail"

  private val hbasePool = new HbasePool("position_log", "hadoop105,hadoop107,hadoop108", "2181")

  //  private val mysqlPool = new MysqlPools(MysqlConfig.USERNAME, MysqlConfig.PASSWORD, MysqlConfig.HOST, MysqlConfig.PORT, MysqlConfig.DBNAME)

  def main(args: Array[String]): Unit = {

    if (args.length < 3)
      logger.info("args length is not correct")

    val receiveMails = Array[InternetAddress](
      new InternetAddress("jcsj_haifeng.wu@cheng95.com", "", "UTF-8"),
      new InternetAddress("dongjun.xu@ifchange.com", "", "UTF-8"),
      new InternetAddress("jcsj_hui.wang@cheng95.com", "", "UTF-8"))

    val master = args(0)
    val topic = args(1)
    val groupId = args(2)
    val appName = "spark-streaming-position-monitor"
    val conf = new SparkConf
    conf.setAppName(appName)
    conf.setMaster(master)
    conf.set("app.logging.name", "spark-streaming-position-monitor")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Durations.minutes(1))
    ssc.checkpoint("/basic_data/position-checks")

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

    val partitioner = new HashPartitioner(3)
    //1、流量统计
    val totalNum = lines.map(rdd => {
      val kafkaValue = rdd._2
      if (StringUtils.isNotBlank(kafkaValue))
        ("position", 1)
      else
        ("position", 0)
    }).combineByKey(s => s, (a: Int, b: Int) => a + b, (a: Int, b: Int) => a + b, partitioner, mapSideCombine = true)
      .reduceByKeyAndWindow((x: Int, y: Int) => x + y, Durations.minutes(5), Durations.minutes(5), 3)

    val totalTime = lines.map(rdd => {
      val kafkaValue = rdd._2
      var map = Map("" -> "")
      var value = ""
      if (StringUtils.isNotBlank(kafkaValue)) {
        try
          value = kafkaValue.substring(kafkaValue.indexOf("t="), kafkaValue.length)
        catch {
          case e: Exception =>
            logger.info("截取kafka传来的值报错," + e.getMessage)
        }
      }
      if (StringUtils.isNotBlank(value)) {
        try {
          map = ParamParseUtil.parse(value)
        } catch {
          case e: Exception => logger.error(value + ",解析报错," + e.getMessage)
        }
      }
      if (map.nonEmpty) {
        var responseTime = 0.0
        try {
          responseTime = map.getOrElse("r", "0").toDouble
        } catch {
          case ex: Exception => logger.info("r转成double报错," + ex.getMessage + ",kafkaValue=" + kafkaValue)
        }
        ("position", responseTime)
      } else
        ("position", 0.0)
    }).combineByKey(s => s, (a: Double, b: Double) => a + b, (a: Double, b: Double) => a + b, partitioner, mapSideCombine = true)
      .reduceByKeyAndWindow((x: Double, y: Double) => x + y, Durations.minutes(5), Durations.minutes(5), 3)

    val pair1 = totalNum.join(totalTime, 3)

    //2、性能监控
    val totalTimeByKAndW = lines.map(rdd => {
      val kafkaValue = rdd._2
      var map = Map("" -> "")
      var value = ""
      if (StringUtils.isNotBlank(kafkaValue)) {
        try
          value = kafkaValue.substring(kafkaValue.indexOf("t="), kafkaValue.length)
        catch {
          case e: Exception =>
            logger.info("截取kafka传来的值报错," + e.getMessage)
        }
      }
      if (StringUtils.isNotBlank(value)) {
        try {
          map = ParamParseUtil.parse(value)
        } catch {
          case e: Exception => logger.error(value + ",解析报错," + e.getMessage)
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
    }).combineByKey(s => s, (a: Double, b: Double) => a + b, (a: Double, b: Double) => a + b, partitioner, mapSideCombine = true)
      .reduceByKeyAndWindow((x: Double, y: Double) => x + y, Durations.minutes(5), Durations.minutes(5), 3)

    val totalNumByKAndW = lines.map(rdd => {
      val kafkaValue = rdd._2
      var map = Map("" -> "")
      var value = ""
      if (StringUtils.isNotBlank(kafkaValue)) {
        try
          value = kafkaValue.substring(kafkaValue.indexOf("t="), kafkaValue.length)
        catch {
          case e: Exception =>
            logger.info("截取kafka传来的值报错," + e.getMessage)
        }
      }
      if (StringUtils.isNotBlank(value)) {
        try {
          map = ParamParseUtil.parse(value)
        } catch {
          case e: Exception => logger.error(value + ",解析报错," + e.getMessage)
        }
      }
      if (map.nonEmpty) {
        val f = map.getOrElse("f", "f")
        val w = map.getOrElse("w", "w")
        val key = f + "+" + w
        (key, 1)
      } else
        ("f+w", 0)
    }).combineByKey(s => s, (a: Int, b: Int) => a + b, (a: Int, b: Int) => a + b, partitioner, mapSideCombine = true)
      .reduceByKeyAndWindow((x: Int, y: Int) => x + y, Durations.minutes(5), Durations.minutes(5), 3)

    //3、稳定性监控
    val totalFailCountByKAndW = lines.map(rdd => {
      val kafkaValue = rdd._2
      var map = Map("" -> "")
      var value = ""
      if (StringUtils.isNotBlank(kafkaValue)) {
        try
          value = kafkaValue.substring(kafkaValue.indexOf("t="), kafkaValue.length)
        catch {
          case e: Exception =>
            logger.info("截取kafka传来的值报错," + e.getMessage)
        }
      }
      if (StringUtils.isNotBlank(value)) {
        try {
          map = ParamParseUtil.parse(value)
        } catch {
          case e: Exception => logger.error(value + ",解析报错," + e.getMessage)
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
    }).combineByKey(s => s, (a: Double, b: Double) => a + b, (a: Double, b: Double) => a + b, partitioner, mapSideCombine = true)
      .reduceByKeyAndWindow((x: Double, y: Double) => x + y, Durations.minutes(5), Durations.minutes(5), 3)

    val pair = totalFailCountByKAndW.join(totalTimeByKAndW, 3).join(totalNumByKAndW, 3)

    //4、报警监控
    val monitorValue = lines.map(rdd => {
      val kafkaValue = rdd._2
      var map = Map("" -> "")
      var value = ""
      if (StringUtils.isNotBlank(kafkaValue)) {
        try
          value = kafkaValue.substring(kafkaValue.indexOf("t="), kafkaValue.length)
        catch {
          case e: Exception =>
            logger.info("截取kafka传来的值报错," + e.getMessage)
        }
      }
      if (StringUtils.isNotBlank(value)) {
        try {
          map = ParamParseUtil.parse(value)
        } catch {
          case e: Exception => logger.error(value + ",解析报错," + e.getMessage)
        }
      }
      if (map.nonEmpty) {
        val f = map.getOrElse("f", "f")
        val w = map.getOrElse("w", "w")
        val s = map.getOrElse("s", "0")
        val key = f + "->" + w
        if (s.equals("0"))
          (key, 1)
        else
          (key, 0)
      } else
        ("f->w", 0)
    }).combineByKey(s => s, (a: Int, b: Int) => a + b, (a: Int, b: Int) => a + b, partitioner, mapSideCombine = true)
      .reduceByKeyAndWindow((x: Int, y: Int) => x + y, Durations.minutes(5), Durations.minutes(5), 3)

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
      redis.select(4)
      rdds.foreach(value => {
        val kafkaValue = value._2
        var map = Map("" -> "")
        var values = ""
        if (StringUtils.isNotBlank(kafkaValue)) {
          try {
            values = kafkaValue.substring(kafkaValue.indexOf("t="), kafkaValue.length)
          } catch {
            case e: Exception =>
              logger.info("截取kafka传来的值报错," + e.getMessage)
          }
        }
        if (StringUtils.isNotBlank(values)) {
          try {
            map = ParamParseUtil.parse(values)
          } catch {
            case e: Exception => logger.error(value + ",解析报错," + e.getMessage)
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
          val hostName = map.getOrElse("hostname", "hostname未知")

          val f = map.getOrElse("f", "f未知")
          val logId = map.getOrElse("logid", String.valueOf(System.currentTimeMillis()))
          val functionMark = "position_" + logId
          //r 频响时间
          if (StringUtils.isNotBlank(time)) {
            val day: String = time.split(" ")(0)
            val successKey: String = POSITION_SUCCESS + "-" + day
            val failKey: String = POSITION_FAIL + "-" + day
            val workSuccessKey: String = w + "-success-" + day
            val workFailKey: String = w + "-fail-" + day
            //统计具体work+c+m
            val wcmSuccessKey: String = w + "-" + c + "-" + m + "-success-" + day
            val wcmFailKey: String = w + "-" + c + "-" + m + "-fail-" + day
            //统计wcm每次的时间[0,1)秒
            val wcmTimeKey1: String = w + "-" + c + "-" + m + "-" + day + "-time1"
            //[1,5)秒
            val wcmTimeKey2: String = w + "-" + c + "-" + m + "-" + day + "-time2"
            //[5,+)秒
            val wcmTimeKey3: String = w + "-" + c + "-" + m + "-" + day + "-time3"
            val host38Key: String = "position-jcsjweb38-" + day
            val host39Key: String = "position-jcsjweb39-" + day
            val host70Key: String = "position-bi70-" + day
            if (hostName.contains("web38")) redisCli.incr(redis, host38Key)
            else if (hostName.contains("web39")) redisCli.incr(redis, host39Key)
            else if (hostName.contains("BI-70")) redisCli.incr(redis, host70Key)
            else logger.info("hostname解析错误:" + hostName)
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
            logger.info("-------------------")
            logger.info(kafkaValue)
            logger.info("-------------------")
            import collection.JavaConversions._
            val hbaseClient = hbasePool.getHbaseClient
            hbaseClient.setTbale("position_log")


            try {
              logger.info("save " + logId + " into hbase")
              hbaseClient.insterByMap(logId, "info", map)
              hbaseClient.saveInsert()
            } catch {
              case e: Exception => logger.info("save into hbase error," + e.getMessage)
            } finally {
              hbasePool.free(hbaseClient)
            }

            val reason = "调用w:" + w + "报错,c:" + c + ",m:" + m
            val sql = "insert into position_log_monitor(`time`,`function_mark`,`is_success`,`response_time`,`work_name`,`alarm_reason`,`f`,`c`,`m`) " + "values(\"" + time + "\",\"" + functionMark + "\"," + s + "," + r + ",\"" + w + "\"," + "\"" + reason + "\",\"" + f + "\",\"" + c + "\",\"" + m + "\")"
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
          val sql = "insert into `position_log_batch_result`(`from_time`,`end_time`,`total_num`,`total_use_time`,`type`)" +
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
        val typeName = "position"
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

    //邮件报警
    monitorValue.foreachRDD(rdd => {
      var count = 0L
      try {
        count = rdd.count()
      } catch {
        case e: Exception => logger.info("rdd count error," + e.getMessage)
      }
      if (count > 0) {
        val number = count.toInt
        val list: Array[(String, Int)] = rdd.take(number)
        for (data <- list) {
          val w = data._1
          val num = data._2
          val localDateTime = LocalDateTime.now()
          val before = localDateTime.minusMinutes(5L)
          val from = dtf.format(before)
          val to = dtf.format(localDateTime)
          if (num >= 50) {
            val reason = from + "到" + to + ",失败次数:" + num
            //发送邮件
            val subject = "POSITION日志报警"
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


