package com.ifchange.spark.streaming.v2.offset

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.log4j.Logger
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


class SparkStreamingOffsetDemo {

}

/**
  * offset -->存入zk中
  */
object SparkStreamingOffsetDemo {

  private val logger = Logger.getLogger(classOf[SparkStreamingOffsetDemo])

  def main(args: Array[String]): Unit = {

    val zkHost = ""
    val brokerList=""
    val zkClient = new ZkClient(zkHost)
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerList,
      "zookeeper.connect" -> zkHost,
      "group.id" -> "testid")
    var kafkaStream: InputDStream[(String, String)] = null
    var offsetRanges = Array[OffsetRange]()
    val conf=new SparkConf()
    conf.setAppName("test-1")
    conf.setMaster("local[3]")
    val sc=new SparkContext(conf)
    val ssc=new StreamingContext(sc,Seconds(5))
    val topic="TEST_TOPIC"

    //创建一个 ZKGroupTopicDirs 对象，对保存
    val topicDirs = new ZKGroupTopicDirs("TEST_TOPIC_spark_streaming_testid", topic)

    //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）
    val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}")

    //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置
    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    //如果保存过 offset，这里更好的做法，还应该和  kafka 上最小的 offset 做对比，不然会报 OutOfRange 的错误
    if (children > 0) {
      for (i <- 0 until children) {
        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
        val tp = TopicAndPartition(topic, i)
        fromOffsets += (tp -> partitionOffset.toLong)  //将不同 partition 对应的 offset 增加到 fromOffsets 中
      }

      //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (topic_name, message) 这样的 tuple
      val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    }
    else {
      //如果未保存，根据 kafkaParam 的配置使用最新或者最旧的 offsetss
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("TEST_TOPIC"))
    }


    kafkaStream.transform{rdd=>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //得到该 rdd 对应 kafka 的消息的 offset
      rdd
    }.map(_._2).foreachRDD(rdd=>{
      for (o <- offsetRanges) {
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.fromOffset.toString)  //将该 partition 的 offset 保存到 zookeeper
      }
      rdd.foreach(s=>println(s))
    })

    ssc.start()
    ssc.awaitTermination()
  }


}