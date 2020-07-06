package com.ifchange.spark.streaming.v2.offset

import kafka.utils.ZKGroupTopicDirs
import org.I0Itec.zkclient.ZkClient
import org.slf4j.LoggerFactory

class TestZkClient {

}
object TestZkClient{

  private val LOG = LoggerFactory.getLogger(classOf[TestZkClient])

  def main(args: Array[String]): Unit = {

//    val zkHost="192.168.8.194:2181,192.168.8.195:2181,192.168.8.196:2181,192.168.8.197:2181"
    val zkHost="192.168.1.107:2181,192.168.1.200:2181"
    val zkClient=new ZkClient(zkHost)
    val groupId="my-group"
    val topic="gsystem_log"
    //创建一个 ZKGroupTopicDirs 对象，对保存
    val topicDirs = new ZKGroupTopicDirs(groupId, topic)
    LOG.info(topicDirs.consumerOwnerDir)
    LOG.info(topicDirs.consumerOffsetDir)
    //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）
//    val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}")
    val data=s"${topicDirs.consumerOffsetDir}"
    LOG.info("data:{}",data)




  }
}
