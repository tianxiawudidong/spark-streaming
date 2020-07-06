package com.ifchange.spark.streaming.v2.jc

import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.Calendar

import com.alibaba.fastjson.JSON
import com.ifchange.sparkstreaming.v1.util.RedisCli
import kafka.serializer.StringDecoder
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import redis.clients.jedis.Jedis

/**
  * cvar中是文本格式，逗号分隔，包含了uid、cids、updated_at、rates、scores、jid、op_time和pageno等字段。
  * 需求：
  * 对每天推荐的jd/cv对进行频次实时统计，结果写入redis；redis数据每天凌晨4点清空（写入redis的时候配置有效时间）。
  * redis的key是${jdid}_${cvid}_daily，value是频次统计值。
  * redis_master_config = {
  * 'host': '192.168.8.118',
  * 'port': 6379,
  * 'password': 'VER7z3Qm2PuP'
  * }
  * redis_salve_config = {
  * 'host': '192.168.8.119',
  * 'port': 6379,
  * 'password': 'VER7z3Qm2PuP'
  * }
  */
class SparkStreamingJcCount {

}

object SparkStreamingJcCount {
  private val logger = Logger.getLogger(classOf[SparkStreamingJcCount])

  private val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")

  private var redisCli = new RedisCli("192.168.8.118", 6379)

  def main(args: Array[String]): Unit = {
    if (args.length < 3)
      logger.info("args length is not correct")

    val master = args(0)
    val topic = args(1)
    val groupId = args(2)
    val appName = "spark-streaming-jc-count"
    val conf = new SparkConf()
    conf.setAppName(appName)
    conf.setMaster(master)
    conf.set("app.logging.name", "spark-streaming-jc-count")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Durations.seconds(5L))
    ssc.checkpoint("/algorithm/jc-checkpoint")

    val kafkaParams = Map("metadata.broker.list" -> "192.168.9.129:9092,192.168.9.130:9092,192.168.9.131:9092,192.168.9.132:9092,192.168.9.133:9092",
      "auto.offset.reset" -> "largest",
      "zookeeper.connect" -> "192.168.9.129:2181,192.168.9.130:2181,192.168.9.131:2181,192.168.9.132:2181,192.168.9.133:2181",
      "group.id" -> groupId)
    val topics = Set(topic)
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val summaryValue = lines.mapPartitions(rdds => {
      var redis: Jedis = null
      try {
        redis = redisCli.getJedis
      } catch {
        case ex: Exception =>
          logger.info(ex.getMessage)
          redisCli = new RedisCli("192.168.8.118", 6379)
          redis = redisCli.getJedis
      }
      redis.auth("VER7z3Qm2PuP")
      redis.select(0)
      rdds.foreach(f = value => {
        val kafkaValue = value._2
        //{"id":121635668,"table":"access_click_log","type":"insert","ts":"2018-12-11 14:12:12","data":{"click":"btn_more_hover","uid":"135065","op_time":"2018-12-11 13:52:37","remote_addr":"115.238.92.254"}}
        if (StringUtils.isNotBlank(kafkaValue)) {
          try {
            val json = JSON.parseObject(kafkaValue)
            if (null != json) {
              val data = json.getString("data")
              //2018-12-11 12:12:11
              val time = json.getString("ts")
              val dataJson = JSON.parseObject(data)
              val cvId = dataJson.getString("cvid")
              val jdId = dataJson.getString("jid")
              val day = time.split(" ")(0)
              val calendar = Calendar.getInstance
              calendar.add(Calendar.DATE, +1)
              calendar.set(Calendar.HOUR_OF_DAY, 2)
              calendar.set(Calendar.MINUTE, 0)
              calendar.set(Calendar.SECOND, 0)
              val date = calendar.getTime
              logger.info(dtf.format(date.toInstant.atZone(ZoneId.systemDefault)))
              val millSeconds = date.getTime
              if (StringUtils.isNotBlank(cvId) && StringUtils.isNotBlank(jdId)) {
                val key = jdId + "_" + cvId + "_" + day
                logger.info(key)
                val value = redisCli.get(key, redis)
                if (StringUtils.isBlank(value)) {
                  redisCli.setPex(redis, key, "1", millSeconds)
                } else {
                  redisCli.incr(redis, key)
                }
              }
            }
          } catch {
            case e: Exception => logger.info("parse kafka value to json error，" + e.getMessage)
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

