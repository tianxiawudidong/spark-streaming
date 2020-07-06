package com.ifchange.spark.streaming.v2

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.ifchange.spark.streaming.v2.mysql.{Mysql, MysqlPools}
import com.ifchange.spark.streaming.v2.util.ParamParseUtil
import com.ifchange.sparkstreaming.v1.common.MysqlConfig
import com.ifchange.sparkstreaming.v1.util.RedisCli
import kafka.serializer.StringDecoder
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * tob
  * 1、流量监控 5分钟 请求总量、请求总时间
  * 2、性能监控 5分钟
  * a、f w 平均响应时间
  * b、w   平均响应时间
  * 3、稳定性监控 5分钟 w 的失败率
  * 4、汇总统计   1分钟汇总一次
  * redis统计
  * Created by Administrator on 2018/1/11.
  */
class SparkStreamingForToBMonitor {

}

object SparkStreamingForToBMonitor {

  private val logger = Logger.getLogger(classOf[SparkStreamingForToBMonitor])

  private val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")

  private var redisCli = new RedisCli("192.168.8.117", 6070)

  private val TOB_SUCCESS = "tob_success"

  private val TOB_FAIL = "tob_fail"

  private val mysqlPool = new MysqlPools(MysqlConfig.USERNAME, MysqlConfig.PASSWORD, MysqlConfig.HOST, MysqlConfig.PORT, MysqlConfig.DBNAME)


  def main(args: Array[String]): Unit = {

    if (args.length < 3)
      logger.error("args length is not correct")

    val master = args(0)
    val topic = args(1)
    val groupId = args(2)
    val appName = "spark-streaming-tob-monitor"
    val conf = new SparkConf()
    conf.setMaster(master)

    conf.setAppName(appName)
    conf.set("app.logging.name", "spark-streaming-tob-monitor")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Durations.minutes(1))
    ssc.checkpoint("/basic_data/tob-checks")

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

    val partitioner = new HashPartitioner(2)
    //1、流量统计
    val totalNum = lines.map(rdd => {
      val kafkaValue = rdd._2
      if (StringUtils.isNotBlank(kafkaValue))
        ("tob", 1)
      else
        ("tob", 0)
    }).combineByKey(s => s, (a: Int, b: Int) => a + b, (a: Int, b: Int) => a + b, partitioner, mapSideCombine = true)
      .reduceByKeyAndWindow((x: Int, y: Int) => x + y, Durations.minutes(5), Durations.minutes(5), 2)

    val totalTime = lines.map(rdd => {
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
        var responseTime = 0.0
        try {
          responseTime = map.getOrElse("r", "0").toDouble
        } catch {
          case ex: Exception => logger.info("r转成double报错," + ex.getMessage + ",kafkaValue=" + kafkaValue)
        }
        ("tob", responseTime)
      } else
        ("tob", 0.0)
    }).combineByKey(s => s, (a: Double, b: Double) => a + b, (a: Double, b: Double) => a + b, partitioner, mapSideCombine = true)
      .reduceByKeyAndWindow((x: Double, y: Double) => x + y, Durations.minutes(5), Durations.minutes(5), 2)

    val pair1 = totalNum.join(totalTime, 2)

    //2、性能监控
    //a、f w 平均响应时间
    val totalTimeByKAndW = lines.map(rdd => {
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
        var f = map.getOrElse("f", "f")
        if (f.startsWith("tob_"))
          f = "tob"
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
      .reduceByKeyAndWindow((x: Double, y: Double) => x + y, Durations.minutes(5), Durations.minutes(5), 2)

    val totalNumByKAndW = lines.map(rdd => {
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
        var f = map.getOrElse("f", "f")
        if (f.startsWith("tob_"))
          f = "tob"
        val w = map.getOrElse("w", "w")
        val key = f + "+" + w
        (key, 1)
      } else
        ("f+w", 0)
    }).combineByKey(s => s, (a: Int, b: Int) => a + b, (a: Int, b: Int) => a + b, partitioner, mapSideCombine = true)
      .reduceByKeyAndWindow((x: Int, y: Int) => x + y, Durations.minutes(5), Durations.minutes(5), 2)

    val pair2 = totalTimeByKAndW.join(totalNumByKAndW, 2)

    //b、w 平均响应时间
    val totalTimeByW = lines.map(rdd => {
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
        val w = map.getOrElse("w", "w")
        var responseTime = 0.0
        try {
          responseTime = map.getOrElse("r", "0").toDouble
        } catch {
          case ex: Exception => logger.info("r转成double报错," + ex.getMessage + ",kafkaValue=" + kafkaValue)
        }
        (w, responseTime)
      } else
        ("w", 0.0)
    }).combineByKey(s => s, (a: Double, b: Double) => a + b, (a: Double, b: Double) => a + b, partitioner, mapSideCombine = true)
      .reduceByKeyAndWindow((x: Double, y: Double) => x + y, Durations.minutes(5), Durations.minutes(5), 2)

    val totalNumByW = lines.map(rdd => {
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
        val w = map.getOrElse("w", "w")
        (w, 1)
      } else
        ("w", 0)
    }).combineByKey(s => s, (a: Int, b: Int) => a + b, (a: Int, b: Int) => a + b, partitioner, mapSideCombine = true)
      .reduceByKeyAndWindow((x: Int, y: Int) => x + y, Durations.minutes(5), Durations.minutes(5), 2)

    //3、稳定性监控
    val totalFailCountByW = lines.map(rdd => {
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
        val w = map.getOrElse("w", "w")
        val s = map.getOrElse("s", "0")
        if (s.equals("0"))
          (w, 1.0)
        else
          (w, 0.0)
      } else
        ("w", 0.0)
    }).combineByKey(s => s, (a: Double, b: Double) => a + b, (a: Double, b: Double) => a + b, partitioner, mapSideCombine = true)
      .reduceByKeyAndWindow((x: Double, y: Double) => x + y, Durations.minutes(5), Durations.minutes(5), 2)

    val pair = totalFailCountByW.join(totalTimeByW, 2).join(totalNumByW, 2)

    //4、汇总统计
    val summaryValue = lines.mapPartitions(rdds => {
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
      redis.select(3)
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
          val time: String = map.getOrElse("t", "")
          //s 是否成功
          val s: String = map.getOrElse("s", "s未知")
          //r 频响时间
          val r: String = map.getOrElse("r", "0")
          val responseTime = r.toDouble
          //w work名
          val w: String = map.getOrElse("w", "w未知")
          val c: String = map.getOrElse("c", "c未知")
          val m: String = map.getOrElse("m", "m未知")
          if (StringUtils.isNotBlank(time)) {
            val day = time.split(" ")(0)
            val successKey = TOB_SUCCESS + "-" + day
            val failKey = TOB_FAIL + "-" + day
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
            //icdc总数
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
            if (s == "1") { //每天tob总的成功次数
              redisCli.incr(redis, successKey)
              //每天tob work成功的次数
              redisCli.incr(redis, workSuccessKey)
              //每天tob work+c+m成功的次数
              redisCli.incr(redis, wcmSuccessKey)
            }
            else { //每天tob总的失败次数
              redisCli.incr(redis, failKey)
              //每天tob work失败的次数
              redisCli.incr(redis, workFailKey)
              //每天tob work+c+m失败的次数
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
        }
      })
      redisCli.returnResource(redis)
      rdds
    }).window(Durations.minutes(1), Durations.minutes(1))

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
          val sql = "insert into `tob_log_batch_result`(`from_time`,`end_time`,`total_num`,`total_use_time`,`type`)" +
            "values(\"" + from + "\",\"" + to + "\"," + num + "," + time + "," + 0 + ")"
          logger.info("sql:" + sql)
          var mysql: Mysql = null
          try {
            mysql = mysqlPool.getMysqlConn
          } catch {
            case ex: Exception => logger.info("get mysql from pool error," + ex.getMessage)
          }
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
    })

    //性能监控
    //f w
    pair2.foreachRDD(rdd => {
      var count = 0L
      try {
        count = rdd.count()
      } catch {
        case e: Exception => logger.info("rdd count error," + e.getMessage)
      }
      if (count > 0) {
        val num = count.toInt
        val list: Array[(String, (Double, Int))] = rdd.take(num)
        val localDateTime = LocalDateTime.now()
        val before = localDateTime.minusMinutes(5L)
        val from = dtf.format(before)
        val to = dtf.format(localDateTime)
        val typeName = "tob"
        val sql = "insert into `echeng_log_request_interval_statistics`(`f`,`work_name`,`from_time`,`end_time`,`avg_response_time`,`type_name`) values(?,?,?,?,?,?)"
        var mysql: Mysql = null
        try {
          mysql = mysqlPool.getMysqlConn
        } catch {
          case ex: Exception => logger.info("get mysql from pool error," + ex.getMessage)
        }
        if (null != mysql) {
          try {
            mysql.batchInsertForT0b1(sql, list, from, to, typeName)
          } catch {
            case ex: java.sql.SQLException => logger.info("insert into mysql error," + ex.getMessage)
          } finally {
            mysqlPool.free(mysql)
          }
        }
      }
    })

    //性能监控 稳定性监控w
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
        val typeName = "tob"
        val sql = "insert into `echeng_log_request_interval_statistics`(`work_name`,`from_time`,`end_time`,`avg_response_time`,`fail_rate`,`type_name`) values(?,?,?,?,?,?)"
        var mysql: Mysql = null
        try {
          mysql = mysqlPool.getMysqlConn
        } catch {
          case ex: Exception => logger.info("get mysql from pool error," + ex.getMessage)
        }
        if (null != mysql) {
          try {
            mysql.batchInsertForTob2(sql, list, from, to, typeName)
          } catch {
            case ex: java.sql.SQLException => logger.info("insert into mysql error," + ex.getMessage)
          } finally {
            mysqlPool.free(mysql)
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
