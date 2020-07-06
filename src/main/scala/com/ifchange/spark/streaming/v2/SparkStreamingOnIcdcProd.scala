package com.ifchange.spark.streaming.v2

import com.alibaba.fastjson.JSON
import com.ifchange.spark.streaming.v2.util.HttpClientUtil
import kafka.serializer.StringDecoder
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * 将icdc php生产日志的请求转发到 java端
  * icdc_log_prod
  * 【1】
  */
class SparkStreamingOnIcdcProd {

}

object SparkStreamingOnIcdcProd {

  private val LOGGER = Logger.getLogger(classOf[SparkStreamingOnIcdcProd])

  private val URL = "http://192.168.9.133:51710/icdc_online"

  def main(args: Array[String]): Unit = {

    if (args.length < 3)
      LOGGER.info("args length is incorrect")

    val master = args(0)
    val topic = args(1)
    val groupId = args(2)
    val appName = "spark-streaming-icdc-log-prod"
    val conf = new SparkConf
    conf.setMaster(master)
    conf.setAppName(appName)
    conf.set("app.logging.name", "spark-streaming-icdc-log-prod")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Durations.minutes(5))
    ssc.checkpoint("/basic_data/icdc-checks")

    val kafkaParams = Map("metadata.broker.list" -> "192.168.8.194:9092,192.168.8.195:9092,192.168.8.196:9092,192.168.8.197:9092",
      "auto.offset.reset" -> "largest",
      "zookeeper.connect" -> "192.168.8.194:2181,192.168.8.195:2181,192.168.8.196:2181,192.168.8.197:2181",
      "group.id" -> groupId)
    val topics = Set(topic)
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)


    val value = lines.filter(data => {
      StringUtils.isNotBlank(data._2)
    }).map(data => {
      val kafkaValue = data._2
      val split = StringUtils.splitByWholeSeparatorPreserveAllTokens(kafkaValue, "\t")
      if (split.length == 6) {
        val `type` = split(3)
        val param = split(5)
        try {
          val jsonObject = JSON.parseObject(param)
          val request = jsonObject.getJSONObject("request")
          val c = request.getString("c")
          if ("API" == `type` && c.contains("resumes")) {
            try {
              HttpClientUtil.httpPost(URL, param)
            } catch {
              case e: Exception =>
                LOGGER.info("http post error:" + e.getMessage)
            }
          }
        } catch {
          case e: Exception =>
            LOGGER.info("param cannot parse json" + e.getMessage)
        }
      }
      kafkaValue
    }).window(Durations.minutes(5), Durations.minutes(5))


    value.foreachRDD(rdd => {
      val count = rdd.count()
      LOGGER.info("count:" + count)
    })

    ssc.start()
    ssc.awaitTermination()

  }
}