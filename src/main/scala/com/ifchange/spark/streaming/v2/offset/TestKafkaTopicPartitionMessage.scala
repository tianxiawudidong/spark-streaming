package com.ifchange.spark.streaming.v2.offset

import kafka.serializer.StringDecoder
import kafka.utils.ZkUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

class TestKafkaTopicPartitionMessage {

}

object TestKafkaTopicPartitionMessage {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[3]")
    conf.setAppName("test")
    val ssc = new StreamingContext(conf, Seconds(10))

    val brokers = "192.168.1.107:9092,192.168.1.200:9092"
    val zkConnection = "192.168.1.200:2181"
    val topic = "test"
    val kafkaParams = Map("metadata.broker.list" -> brokers,
      "auto.offset.reset" -> "largest",
      "zookeeper.connect" -> zkConnection,
      "group.id" -> "test-11")
    val topics = Set(topic)
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    var offsetRanges = Array[OffsetRange]()

    //print offset partition
    lines.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }).map(_._2).foreachRDD(rdd => {
      println("-------------------------")
      rdd.foreach(str => {
        for (o <- offsetRanges) {
          println("value:%s,offset:%s,partition:%s".format(str, o.fromOffset, o.partition))
        }
      })
      println("-------------------------")
    })
    ssc.start()
    ssc.awaitTermination()

  }

}
