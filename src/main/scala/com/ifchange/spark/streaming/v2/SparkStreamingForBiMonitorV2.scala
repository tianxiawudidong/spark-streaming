package com.ifchange.spark.streaming.v2

import com.ifchange.spark.streaming.v2.util.ParamParseUtil
import com.ifchange.sparkstreaming.v1.util.RedisCli
import kafka.serializer.StringDecoder
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import redis.clients.jedis.Jedis


/**
  * bi log监控
  * 1、汇总统计
  * c+m
  * 总数
  * redis  5分钟汇总一次
  * Created by xudongjun on 2018/8/15.
  */
class SparkStreamingForBiMonitorV2 {

}

object SparkStreamingForBiMonitorV2 {

  private val logger = Logger.getLogger(classOf[SparkStreamingForBiMonitorV2])

  private var redisCli = new RedisCli("192.168.8.117", 6070)

  def main(args: Array[String]): Unit = {

    if (args.length < 3)
      logger.info("args length is incorrect")

    val master = args(0)
    val topic = args(1)
    val groupId = args(2)
    val appName = "spark-streaming-bi-monitor"
    val conf = new SparkConf
    conf.setMaster(master)
    conf.setAppName(appName)
    conf.set("app.logging.name", "spark-streaming-bi-monitor")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Durations.minutes(5))
    ssc.checkpoint("/basic_data/bi-checks")

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

    //1、汇总统计
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
      redis.select(21)
      rdds.foreach(rdd => {
        val kafkaValue = rdd._2
        //BiGo> t=2018-09-19 10:27:20&logid=1b408084aa745001420&f=bigo&w=/bi_zhichang&c=bizhichang&m=get_company_bi_info&r=19ms
        if (StringUtils.isNotBlank(kafkaValue) && kafkaValue.contains("t=") && kafkaValue.contains("&f=")) {
          val data = kafkaValue.substring(kafkaValue.indexOf("t="), kafkaValue.length)
          logger.info(data)
          if (StringUtils.isNotBlank(data)) {
            try {
              val map = ParamParseUtil.parse(data)
              val times = map.getOrElse("t", "")
              //            val f = map.getOrElse("f", "f")
              //            val w = map.getOrElse("w", "w")
              val c = map.getOrElse("c", "c")
              val m = map.getOrElse("m", "m")
              val responseTime = map.getOrElse("r", "0").replace("ms", "").toDouble
              val day = times.split(" ")(0)
              //统计bi log的总请求数
              val biCountKey = "bi-total-" + day
              //统计cm每次的时间[0,1)秒
              val cmTimeKey1 = c + "-" + m + "-" + day + "-time1"
              //[1,5)秒
              val cmTimeKey2 = c + "-" + m + "-" + day + "-time2"
              //[5,+)秒
              val cmTimeKey3 = c + "-" + m + "-" + day + "-time3"
              if (!redis.exists(biCountKey)) redisCli.set(redis, biCountKey, "0")
              if (!redis.exists(cmTimeKey1)) redisCli.set(redis, cmTimeKey1, "0")
              if (!redis.exists(cmTimeKey2)) redisCli.set(redis, cmTimeKey2, "0")
              if (!redis.exists(cmTimeKey3)) redisCli.set(redis, cmTimeKey3, "0")
              redisCli.incr(redis, biCountKey)
              if (responseTime >= 0 && responseTime < 1000) {
                redisCli.incr(redis, cmTimeKey1)
              } else if (responseTime >= 1000 && responseTime < 5000) {
                redisCli.incr(redis, cmTimeKey2)
              } else {
                redisCli.incr(redis, cmTimeKey3)
              }
            } catch {
              case e: Exception => logger.info(kafkaValue + ",解析报错," + e.getMessage)
            }
          }
        }
      })
      redisCli.returnResource(redis)
      rdds
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




