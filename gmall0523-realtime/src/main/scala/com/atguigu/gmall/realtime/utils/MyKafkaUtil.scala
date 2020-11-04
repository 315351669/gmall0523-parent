package com.atguigu.gmall.realtime.utils


import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.StreamingContext

object MyKafkaUtil {

  private val properties = MyPropertiesUtil.load("config.properties")

  private val broker_list = properties.getProperty("kafka.broker.list")


  // * 初始化  KAFKAParams

  var kafkaParam = collection.mutable.Map(


    //集群地址
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_list,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],

    //消费者组
    ConsumerConfig.GROUP_ID_CONFIG -> "gmall0523_group",
    //偏移量
    //1.偏移量自动重置时  从哪儿开始
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
    //2.偏移量维护: 自动 or 手动
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)


  )

  //  最后来看  好像没啥用

  def getKafkaStream(topic: String, ssc: StreamingContext) = {

    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
    )

    dStream

  }


  //重载  自己指定  消费者组   谁   消费  哪个  主体
  def getKafkaStream(topic: String, ssc: StreamingContext, groupId: String) = {
    kafkaParam(ConsumerConfig.GROUP_ID_CONFIG) = groupId
    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))
    dStream
  }

  //重载  指定消费主体到哪个位置  -- 谁(groupid)  消费  哪个主体(topic)的哪个分区(partition)  的  哪个位置(offset) 了

  def getKafkaStream(topic: String,ssc:StreamingContext,offsets:Map[TopicPartition,Long],groupId:String)
  : InputDStream[ConsumerRecord[String,String]]={
    kafkaParam("group.id")=groupId
    val dStream = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(topic),kafkaParam,offsets))
    dStream
  }



}