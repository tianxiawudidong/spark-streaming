package com.ifchange.spark.streaming.v2.offset

import com.ifchange.spark.streaming.v2.mysql.Mysql
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}

class SparkStreamingReadDateFromOffset {

}

object SparkStreamingReadDateFromOffset {
  private val logger = Logger.getLogger(classOf[SparkStreamingReadDateFromOffset])

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[6]")
    conf.setAppName("test")
    val ssc = new StreamingContext(conf, Seconds(10))

    val brokers = "192.168.1.107:9092,192.168.1.200:9092"
    val zkConnection = "192.168.1.200:2181"
    val topic = "test"
    val kafkaParams = Map("metadata.broker.list" -> brokers,
      "auto.offset.reset" -> "smallest",
      "zookeeper.connect" -> zkConnection,
      "group.id" -> "test-3")
    //read data from mysqls
    val mysql = new Mysql("root", "admin", "test", "192.168.3.206", 3306)
    val sql = "select * from `test`.`offset_message` where topic='" + topic + "'"
    println(sql)
    val partitionOffsetMapList = mysql.executeQueryOffset(sql)
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    //mysql有topic partition offset
    //从mysql读取相关信息 然后从指定位置处开始消费
    val size = partitionOffsetMapList.size()
    for (i <- 0 until size) {
      val partitionOffsetMap = partitionOffsetMapList.get(i)
      val partition = partitionOffsetMap.get("partition")
      val offset = partitionOffsetMap.get("offset")
      val tp = TopicAndPartition(topic, partition.toInt)
      fromOffsets += (tp -> offset.toLong) //将不同 partition 对应的 offset 增加到 fromOffsets 中
    }
    println(fromOffsets)
    //这个会将 kafka 的消息进行 transform，最终 kafka 的数据都会变成 (topic_name, message) 这样的 tuple
    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)


    var offsetRanges = Array[OffsetRange]()

    kafkaStream.transform({ rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }).map(rdd=>{
      logger.info(rdd._1)
      logger.info(rdd._2)
    }).foreachRDD { rdd =>
      for (o <- offsetRanges) {
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
    }
    ssc.start()
    ssc.awaitTermination()

  }

}


