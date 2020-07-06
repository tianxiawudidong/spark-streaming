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
  * ICDC
  * 1、流量监控   总的请求量和时间    【5分钟】
  * 2、性能监控   f worker 的平均响应时间 【5分钟】
  * 3、稳定性监控 f worker 失败率 【5分钟】
  * 4、报警监控   失败次数 【5分钟 50次】
  * 5、汇总统计   5分钟汇总一次
  * redis统计
  * 错误、超时保存 hbase mysql
  * Created by Administrator on 2018/1/10.
  * update at 5/25
  * 使用mysql 替换 mysql pool 减少连接数
  */
class SparkStreamingForIcdcMonitorV2 {

}

object SparkStreamingForIcdcMonitorV2 {
  private val logger = Logger.getLogger(classOf[SparkStreamingForIcdcMonitorV2])

  private val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")

  private var redisCli = new RedisCli("192.168.8.117", 6070)

  private val ICDC_SUCCESS = "icdc_success"

  private val ICDC_FAIL = "icdc_fail"

  private val hbasePool = new HbasePool("icdc_log", "hadoop105,hadoop107,hadoop108", "2181")

  //  private val mysqlPool = new MysqlPools(MysqlConfig.USERNAME, MysqlConfig.PASSWORD, MysqlConfig.HOST, MysqlConfig.PORT, MysqlConfig.DBNAME)

  def main(args: Array[String]): Unit = {

    if (args.length < 3)
      logger.info("args length is incorrect")

    val receiveMails = Array[InternetAddress](
      new InternetAddress("dongqing.shi@ifchange.com", "", "UTF-8"),
      new InternetAddress("dongjun.xu@ifchange.com", "", "UTF-8"),
      new InternetAddress("jiqing.sun@ifchange.com", "", "UTF-8"),
      new InternetAddress("long.zhang@ifchange.com", "", "UTF-8"),
      new InternetAddress("zhi.liu@ifchange.com", "", "UTF-8")
    )


    val master = args(0)
    val topic = args(1)
    val groupId = args(2)
    val appName = "spark-streaming-icdc-monitor"
    val conf = new SparkConf
    conf.setMaster(master)
    conf.setAppName(appName)
    conf.set("app.logging.name", "spark-streaming-icdc-monitor")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Durations.minutes(1))
    ssc.checkpoint("/basic_data/icdc-checks")

    val kafkaParams = Map("metadata.broker.list" -> "192.168.8.194:9092,192.168.8.195:9092,192.168.8.196:9092,192.168.8.197:9092",
      "auto.offset.reset" -> "largest",
      "zookeeper.connect" -> "192.168.8.194:2181,192.168.8.195:2181,192.168.8.196:2181,192.168.8.197:2181",
      "group.id" -> groupId)
    val topics = Set(topic)
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)


    //kafka 0.11
    //import org.apache.spark.streaming.kafka010._
    //import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
    //import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
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

    //1、流量监控(总量+时间)
    val totalNum = lines.filter(data => {
      StringUtils.isNotBlank(data._2)
    }).map(data => {
      val kafkaValue = data._2
      if (StringUtils.isNotBlank(kafkaValue))
        ("icdc", 1)
      else
        ("icdc", 0)
    }).combineByKey(s => s, (a: Int, b: Int) => a + b, (a: Int, b: Int) => a + b, partitioner, mapSideCombine = true)
      .reduceByKeyAndWindow((x: Int, y: Int) => x + y, Durations.minutes(5), Durations.minutes(5), 3)

    val totalTime = lines.filter(data => {
      StringUtils.isNotBlank(data._2)
    }).map(data => {
      val kafkaValue = data._2
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
        ("icdc", responseTime)
      } else {
        ("icdc", 0.0)
      }
    }).combineByKey(s => s, (a: Double, b: Double) => a + b, (a: Double, b: Double) => a + b, partitioner, mapSideCombine = true)
      .reduceByKeyAndWindow((x: Double, y: Double) => x + y, Durations.minutes(5), Durations.minutes(5), 3)

    val pair1 = totalNum.join(totalTime, 3)

    //2、性能监控（f、w、平均响应时间=总的时间/总的个数）
    //(f+w,time)
    val totalTimeByFAndW = lines.filter(data => {
      StringUtils.isNotBlank(data._2)
    }).map(str => {
      val kafkaValue = str._2
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
      } else {
        ("f+w", 0.0)
      }
    }).combineByKey(s => s, (a: Double, b: Double) => a + b, (a: Double, b: Double) => a + b, partitioner, mapSideCombine = true)
      .reduceByKeyAndWindow((x: Double, y: Double) => x + y, Durations.minutes(5), Durations.minutes(5), 3)

    val totalNumByFAndW = lines.filter(data => {
      StringUtils.isNotBlank(data._2)
    }).map(str => {
      val kafkaValue = str._2
      var map = Map[String, String]()
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
      } else {
        ("f+w", 0)
      }
    }).combineByKey(s => s, (a: Int, b: Int) => a + b, (a: Int, b: Int) => a + b, partitioner, mapSideCombine = true)
      .reduceByKeyAndWindow((x: Int, y: Int) => x + y, Durations.minutes(5), Durations.minutes(5), 3)

    //3、稳定性监控
    val totalFailCountByFAndW = lines.filter(data => {
      StringUtils.isNotBlank(data._2)
    }).map(str => {
      val kafkaValue = str._2
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
      } else {
        ("f+w", 0.0)
      }
    }).combineByKey(s => s, (a: Double, b: Double) => a + b, (a: Double, b: Double) => a + b, partitioner, mapSideCombine = true)
      .reduceByKeyAndWindow((x: Double, y: Double) => x + y, Durations.minutes(5), Durations.minutes(5), 3)

    val pair = totalFailCountByFAndW.join(totalTimeByFAndW, 3).join(totalNumByFAndW, 3)

    //4、报警监控
    val monitorValue = lines.filter(data => {
      StringUtils.isNotBlank(data._2)
    }).map(str => {
      val kafkaValue = str._2
      var map = Map("" -> "")
      if (StringUtils.isNotBlank(kafkaValue)) {
        try {
          map = ParamParseUtil.parse(kafkaValue)
        } catch {
          case e: Exception => logger.info(kafkaValue + ",解析报错," + e.getMessage)
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
        if (responseTime > 5000)
          (w, 1)
        else
          (w, 0)
      } else
        ("w", 0)
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
      redis.select(0)
      rdds.foreach(rdd => {
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
          val time = map.getOrElse("t", "")
          val s = map.getOrElse("s", "0")
          val hostname = map.getOrElse("hostname", "hostname")
          val r = map.getOrElse("r", "0")
          val responseTime = r.toInt
          val w = map.getOrElse("w", "w")
          val c = map.getOrElse("c", "c")
          val m = map.getOrElse("m", "m")
          val f = map.getOrElse("f", "f未知")
          val rowKey = map.getOrElse("logid", "")
          val functionMark = "icdc_" + rowKey
          val errNo = map.getOrElse("err_no", "")
          val errMsg = map.getOrElse("err_msg", "")
          var reason = ""
          if (StringUtils.isNotBlank(time)) {
            val day = time.split(" ")(0)
            val successKey = ICDC_SUCCESS + "-" + day
            val failKey = ICDC_FAIL + "-" + day
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
            val host39Key = "icdc-jcsjweb39-" + day
            val host38Key = "icdc-jcsjweb38-" + day
            val host70Key = "icdc-bi70-" + day
            val search28Key = "icdc-search28-" + day
            val search29Key = "icdc-search29-" + day
            if (!redis.exists(successKey)) redisCli.set(redis, successKey, "0")
            if (!redis.exists(failKey)) redisCli.set(redis, failKey, "0")
            if (!redis.exists(workSuccessKey)) redisCli.set(redis, workSuccessKey, "0")
            if (!redis.exists(workFailKey)) redisCli.set(redis, workFailKey, "0")
            if (!redis.exists(wcmSuccessKey)) redisCli.set(redis, wcmSuccessKey, "0")
            if (!redis.exists(wcmFailKey)) redisCli.set(redis, wcmFailKey, "0")
            if (!redis.exists(wcmTimeKey1)) redisCli.set(redis, wcmTimeKey1, "0")
            if (!redis.exists(wcmTimeKey2)) redisCli.set(redis, wcmTimeKey2, "0")
            if (!redis.exists(wcmTimeKey3)) redisCli.set(redis, wcmTimeKey3, "0")
            if (!redis.exists(host39Key)) redisCli.set(redis, host39Key, "0")
            if (!redis.exists(host38Key)) redisCli.set(redis, host38Key, "0")
            if (!redis.exists(host70Key)) redisCli.set(redis, host70Key, "0")
            if (!redis.exists(search28Key)) redisCli.set(redis, search28Key, "0")
            if (!redis.exists(search29Key)) redisCli.set(redis, search29Key, "0")
            hostname match {
              case "jcsj-web38" => redisCli.incr(redis, host38Key)
              case "jcsj-web39" => redisCli.incr(redis, host39Key)
              case "BI-70.ifchange" => redisCli.incr(redis, host70Key)
              case "search28" => redisCli.incr(redis, search28Key)
              case "search29" => redisCli.incr(redis, search29Key)
              case _ => logger.info(hostname)
            }
            s match {
              case "1" =>
                redisCli.incr(redis, successKey)
                redisCli.incr(redis, workSuccessKey)
                redisCli.incr(redis, wcmSuccessKey)
              case "0" =>
                redisCli.incr(redis, failKey)
                redisCli.incr(redis, workFailKey)
                redisCli.incr(redis, wcmFailKey)
              case _ =>
                logger.info("s没有解析出来:" + kafkaValue)
            }
            if (responseTime >= 0 && responseTime < 1000)
              redisCli.incr(redis, wcmTimeKey1)
            else if (responseTime >= 1000 && responseTime < 5000)
              redisCli.incr(redis, wcmTimeKey2)
            else
              redisCli.incr(redis, wcmTimeKey3)
          }
          if (s.equals("0") || responseTime >= 5000) {
            logger.info("------------------")
            logger.info(kafkaValue)
            logger.info("------------------")
            if (s.equals("0"))
              reason = "调用w:" + w + ",调用失败" + "，errNo:" + errNo + ",errMsg:" + errMsg
            if (responseTime >= 5000)
              reason = "调用w:" + w + ",频响时间超过5秒"
            //将错误信息保存到hbase和mysql
            if (StringUtils.isNotBlank(rowKey)) {
              val hbaseClient = hbasePool.getHbaseClient
              hbaseClient.setTbale("icdc_log")
              import collection.JavaConversions._
              try {
                hbaseClient.insterByMap(rowKey, "info", map)
                hbaseClient.saveInsert()
              } catch {
                case e: Exception => logger.info("save " + rowKey + " into hbase error," + e.getMessage)
              } finally {
                hbasePool.free(hbaseClient)
              }
            }

            val sql = "insert into log_monitor(`time`,`function_mark`,`is_success`,`response_time`,`work_name`,`alarm_reason`,`type`,`f`,`c`,`m`) " + "values(\"" + time + "\",\"" + functionMark + "\"," + s + "," + r + ",\"" + w + "\"," + "\"" + reason + "\"," + 0 + ",\"" + f + "\",\"" + c + "\",\"" + m + "\"" + ")"
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

    //流量监控 写入mysql
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
        val typeName = "icdc"
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

    //报警监控 发送邮件
    monitorValue.foreachRDD(rdd => {
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
        for (data <- list) {
          val w = data._1
          val num = data._2
          val reason = from + "到" + to + ",超时次数:" + num
          if (num >= 50) {
            //发送邮件
            val subject = "ICDC日志报警"
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


