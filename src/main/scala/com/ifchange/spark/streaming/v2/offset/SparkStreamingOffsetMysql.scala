package com.ifchange.spark.streaming.v2.offset

import com.ifchange.spark.streaming.v2.mysql.Mysql
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

class SparkStreamingOffsetMysql {

}

/**
  * 将kafka topic partition offset存入mysql中，
  * 启动的时候从mysql拿相关的partition offset
  * 每次batch再将offset存入mysql中
  */
object SparkStreamingOffsetMysql {
  private val logger = Logger.getLogger(classOf[SparkStreamingOffsetMysql])

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
    val topics = Set(topic)
    //read data from mysql
    val mysql = new Mysql("root", "admin", "test", "192.168.3.206", 3306)
    val sql = "select * from `test`.`offset_message` where topic='" + topic + "'"
    println(sql)
    val partitionOffsetMapList = mysql.executeQueryOffset(sql)

    //    [K, V, KD <: kafka.serializer.Decoder[K], VD <: kafka.serializer.Decoder[V], R]
    //    (ssc : org.apache.spark.streaming.StreamingContext,
    //      kafkaParams : scala.Predef.Map[scala.Predef.String, scala.Predef.String],
    //      fromOffsets : scala.Predef.Map[kafka.common.TopicAndPartition, scala.Long],
    //      messageHandler : scala.Function1[kafka.message.MessageAndMetadata[K, V], R]

    var kafkaStream: InputDStream[(String, String)] = null


    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    //mysql有topic partition offset
    //从mysql读取相关信息 然后从指定位置处开始消费
    if (null != partitionOffsetMapList && partitionOffsetMapList.size() > 0) {
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
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    } else {
      //mysql 没有topic partition offset信息
      //        [K, V, KD <: kafka.serializer.Decoder[K], VD <: kafka.serializer.Decoder[V]]
      //      (ssc : org.apache.spark.streaming.StreamingContext,
      //        kafkaParams : scala.Predef.Map[scala.Predef.String, scala.Predef.String],
      //        topics : scala.Predef.Set[scala.Predef.String])
      //      (implicit evidence$19 : scala.reflect.ClassTag[K], evidence$20 : scala.reflect.ClassTag[V], evidence$21 : scala.reflect.ClassTag[KD], evidence$22 : scala.reflect.ClassTag[VD])
      //      : org.apache.spark.streaming.dstream.InputDStream[scala.Tuple2[K, V]] = { /* compiled code */ }
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    }

    //处理逻辑
    //    var range: Array[OffsetRange] = null
    //     kafkaStream.transform(rdd => {
    //      range = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    //      rdd
    //    }).map(value => {
    //      val kafkaValue = value._2
    //      println("---------------------")
    //      println(kafkaValue)
    //      println("---------------------")
    //    }).foreachRDD(rdd => {
    //      val saveOffsetSql = "update `offset_message` set `offset`=? where topic=? and `partition`=? "
    //      mysql.batchInsertForOffset(saveOffsetSql, range, topic)
    //      rdd.count()
    //    })

     val result=kafkaStream.map(rdd=>{
       val kafkaValue=rdd._2
       logger.info(kafkaValue)
       kafkaValue
     })

    result.print()


//    kafkaStream.foreachRDD(rdd => {
//      val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      rdd.foreach(line => {
//        val o: OffsetRange = offsetsList(TaskContext.get.partitionId)
//        logger.info("++++++++++++++++++++++++++++++此处记录offset+++++++++++++++++++++++++++++++++++++++")
//        logger.info(o.partition + "--->" + o.fromOffset)
//        logger.info("+++++++++++++++++++++++++++++++此处消费数据操作++++++++++++++++++++++++++++++++++++++")
//        logger.info("The kafka  line is " + line)
//      })
//    })
    ssc.start()
    ssc.awaitTermination()

  }


}
