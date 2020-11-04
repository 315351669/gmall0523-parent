package com.atguigu.gmall.realtime.dim

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.UserInfo
import com.atguigu.gmall.realtime.utils.{MyKafkaUtil, OffsetManagerUtil, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UserInfoApp {

  def main(args: Array[String]): Unit = {


    //1.1  从 kafka  读取 变化的 数据
    //1.1.1  准备环境
    val sparkConf = new SparkConf().setAppName("UserInfoApp").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val topic = "ods_user_info"
    val groupId = "user_info_group"

    val offsetMap = OffsetManagerUtil.getOffset(topic,groupId)

    //读取数据
    var userInfoInputDStream : InputDStream[ConsumerRecord[String,String]] = null
    if(offsetMap != null && offsetMap.size > 0){
      //如果 能够从 redis  获取到  消费过的偏移量  就从这个偏移量的位置开始读取数据
      userInfoInputDStream = MyKafkaUtil.getKafkaStream(topic,ssc,offsetMap,groupId)
    } else{
      //如果 没有从 redis 获取到偏移量 信息 ,  那么从kafka记录的 偏移量 位置 开始读取数据
      userInfoInputDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }

    //  读取完  数据  ::   记录 此次  读取数据的 偏移量 位置信息

    var offsetRanges = Array.empty[OffsetRange]

    val userInfoDStream = userInfoInputDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //1.2  处理数据(处理数据好像都用foreachRDD  不太确定 等以后再确定  |||  不用foreachRDD  分开处理 也是可以的)   并  保存到  Hbase
//    userInfoDStream.userInfoRecordRDD{
     userInfoDStream.foreachRDD{
       rdd => {
         val userInfoRDD = rdd.map {
           userInfoRecord => {
             val userInfo = JSON.parseObject(userInfoRecord.value(), classOf[UserInfo])
             //把生日转成年龄
             val formattor = new SimpleDateFormat("yyyy-MM-dd")
             val date: Date = formattor.parse(userInfo.birthday)
             val curTs: Long = System.currentTimeMillis()
             val betweenMs = curTs - date.getTime
             val age = betweenMs/1000L/60L/60L/24L/365L
             if (age < 20) {
               userInfo.age_group = "20岁及以下"
             } else if (age > 30) {
               userInfo.age_group = "30岁以上"
             } else {
               userInfo.age_group = "21岁到30岁"
             }

             if (userInfo.gender == "M") {
               userInfo.gender_name = "男"
             } else {
               userInfo.gender_name = "女"
             }
             userInfo
           }
         }


         import org.apache.phoenix.spark._
         userInfoRDD.saveToPhoenix(
           "GMALL0523_USER_INFO",
           Seq("ID","USER_LEVEL","BIRTHDAY","GENDER","AGE_GROUP","GENDER_NAME"),
           new Configuration,
           Some("hadoop102,hadoop103,hadoop104:2181")
         )

         // 保存 偏移量

         OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)


       }
     }

    ssc.start()
    ssc.awaitTermination()

  }

}
