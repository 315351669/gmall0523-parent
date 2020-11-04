package com.atguigu.gmall.realtime.utils

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.common.TopicPartition

/**
  *
  * @ClassName OffsetManagerM
  * @Description
  * @Author R
  * @Date 11/3/2020 7:40 PM
  * @Version 1.0
  **/
object OffsetManagerM {
  /**
   * @Author   
   * @Description   
   * @Date 11/3/2020 7:42 PM
   * @Param [topic, consumerGroupId]
   * @return scala.collection.immutable.Map<org.apache.kafka.common.TopicPartition,java.lang.Object> 
   */
   
  def getOffset(topic: String, consumerGroupId: String): Map[TopicPartition, Long] = {
    val sql="select group_id,topic,topic_offset,partition_id from offset_0523" +
      " where topic='"+topic+"' and group_id='"+consumerGroupId+"'"

    val jsonObjList: List[JSONObject] = MySQLUtil.queryList(sql)

    val topicPartitionList: List[(TopicPartition, Long)] = jsonObjList.map {
      jsonObj =>{
        val topicPartition: TopicPartition = new TopicPartition(topic, jsonObj.getIntValue("partition_id"))
        val offset: Long = jsonObj.getLongValue("topic_offset")
        (topicPartition, offset)
      }
    }
    val topicPartitionMap: Map[TopicPartition, Long] = topicPartitionList.toMap
    topicPartitionMap
  }
}
