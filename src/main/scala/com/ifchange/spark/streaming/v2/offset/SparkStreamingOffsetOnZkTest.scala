package com.ifchange.spark.streaming.v2.offset

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.slf4j.LoggerFactory

/**
  * offset zk
  */
class SparkStreamingOffsetOnZkTest {

}

object SparkStreamingOffsetOnZkTest {

  private val LOG = LoggerFactory.getLogger(classOf[SparkStreamingOffsetOnZkTest])

  def main(args: Array[String]): Unit = {
    val zkHost="192.168.8.194:2181,192.168.8.195:2181,192.168.8.196:2181,192.168.8.197:2181"
    val zkClient=new ZkClient(zkHost)

    val master = args(0)
    val topic = args(1)
    val groupId = args(2)
    val appName = "spark-streaming-offset-test"
    val conf = new SparkConf
    conf.setMaster(master)
    conf.setAppName(appName)
    conf.set("app.logging.name", "spark-streaming-offset-test")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Durations.seconds(30L))
    ssc.checkpoint("/basic_data/gsystem-checks2")

    val kafkaParams = Map("metadata.broker.list" -> "192.168.8.194:9092,192.168.8.195:9092,192.168.8.196:9092,192.168.8.197:9092",
      "auto.offset.reset" -> "smallest",
      "zookeeper.connect" ->zkHost,
      "group.id" -> groupId,
      "enable.auto.commit" -> "false")
    var kafkaStream: InputDStream[(String, String)] = null
    var offsetRanges = Array[OffsetRange]()
    //创建一个 ZKGroupTopicDirs 对象，对保存
    val topicDirs = new ZKGroupTopicDirs(groupId, topic)
    val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}")
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    LOG.info("------------------------------------------------------------------------")
    LOG.info("children:{}",children)
    LOG.info("------------------------------------------------------------------------")
    //如果保存过 offset，这里更好的做法，还应该和  kafka 上最小的 offset 做对比，不然会报 OutOfRange 的错误
    if (children > 0) {
      for (i <- 0 until children) {
        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/$i")
        val tp = TopicAndPartition(topic, i)
        fromOffsets += (tp -> partitionOffset.toLong)  //将不同 partition 对应的 offset 增加到 fromOffsets 中
      }
      //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (topic_name, message) 这样的 tuple
      val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    }
    else {
      //如果未保存，根据 kafkaParam 的配置使用最新或者最旧的 offsetss
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topic))
    }

    val result=kafkaStream.map(rdd=>{
      val key=rdd._1
      val value=rdd._2
      LOG.info("value:{}",value)
      value
    })

    //save offset to zk
    kafkaStream.transform({rdd=>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //得到该 rdd 对应 kafka 的消息的 offset
      rdd
    }).map(_._2).foreachRDD(rdd=>{
      LOG.info("==========================================")
      LOG.info("save offset to zk...")
      for (o <- offsetRanges) {
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.fromOffset.toString)  //将该 partition 的 offset 保存到 zookeeper
      }
      rdd.foreach(s=>println(s))
      LOG.info("==========================================")
    })

    val count=result.count()
    LOG.info("count:{}",count.toString)

    ssc.start()
    ssc.awaitTermination()

  }

}
