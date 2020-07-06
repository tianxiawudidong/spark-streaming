package com.ifchange.spark.streaming.v2.offset

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.{BrokerNotAvailableException, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.utils.{ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient


class MyKafkaUtil {

  def getOffsets(zkServers: String, groupId: String, topics: String): Map[TopicAndPartition, Long] ={
    //  val zkClient = new ZkClient(zkServers)
    // 必须要使用带有ZkSerializer参数的构造函数来构造，否则在之后使用ZkUtils的一些方法时会出错，而且在向zookeeper中写入数据时会导致乱码
    // org.I0Itec.zkclient.exception.ZkMarshallingError: java.io.StreamCorruptedException: invalid stream header: 7B227665
    val zkClient = new ZkClient(zkServers,Integer.MAX_VALUE,10000, ZKStringSerializer)
    val topicPartitions = ZkUtils.getPartitionsForTopics(zkClient, topics.split(",")).head
    val topic = topicPartitions._1
    val partitions = topicPartitions._2
    val topicDirs = new ZKGroupTopicDirs(groupId, topic)
    var offsetsMap: Map[TopicAndPartition, Long] = Map()
    partitions.foreach { partition =>
      val zkPath = s"${topicDirs.consumerOffsetDir}/$partition" // /consumers/[groupId]/offsets/[topic]/partition
      //      ZkUtils.makeSurePersistentPathExists(zkClient, zkPath) // 如果zookeeper之前不存在该目录，就直接创建
      val tp = TopicAndPartition(topic, partition)
      // 得到kafka中该partition的最早时间的offset
      val offsetForKafka = getOffsetFromKafka(zkServers, tp, OffsetRequest.EarliestTime)
      // 得到zookeeper中存储的该partition的offset
      val offsetForZk = ZkUtils.readDataMaybeNull(zkClient, zkPath) match {
        case (Some(offset), stat) =>
          Some(offset)
        case (None, stat) =>  // zookzeeper中未存储偏移量
          None
      }

      if (offsetForZk.isEmpty || offsetForZk.get.toLong < offsetForKafka){// 如果zookzeeper中未存储偏移量或zookzeeper中存储的偏移量已经过期
        println("Zookeeper don't save offset or offset has expire!")
        offsetsMap += (tp -> offsetForKafka)
      }else{
        offsetsMap += (tp -> offsetForZk.get.toLong)
      }
    }
    println(s"offsets: $offsetsMap")
    offsetsMap
  }

  private def getOffsetFromKafka(zkServers: String, tp: TopicAndPartition, time: Long):Long = {

    val zkClient = new ZkClient(zkServers, Integer.MAX_VALUE, 10000, ZKStringSerializer)
    val request = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(time, 1)))
    //  得到每个分区的leader（某个broker）
    ZkUtils.getLeaderForPartition(zkClient, tp.topic, tp.partition) match {
      case Some(brokerId) =>
        // 根据brokerId得到leader这个broker的详细信息
        ZkUtils.getBrokerInfo(zkClient, brokerId) match {
          case Some(broker) =>
            // 使用leader的host和port来创建一个SimpleConsumer
            val consumer = new SimpleConsumer(broker.host, broker.port, 10000, 100000, "getOffsetShell")
            val offsets = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(tp).offsets
            offsets.head
          case None =>
            throw new BrokerNotAvailableException("Broker id %d does not exist".format(brokerId))
        }
      case None =>
        throw new Exception("No broker for partition %s - %s".format(tp.topic, tp.partition))
    }
  }


}
