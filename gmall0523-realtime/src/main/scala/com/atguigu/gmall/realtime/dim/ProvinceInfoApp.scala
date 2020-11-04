package com.atguigu.gmall.realtime.dim

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.ProvinceInfo
import com.atguigu.gmall.realtime.utils.{MyKafkaUtil, OffsetManagerUtil, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

// 订单 关联 省份  维度表
object ProvinceInfoApp {
  def main(args: Array[String]): Unit = {

    //1 从kafka读取  省份 数据(用 SparkStreaming)
    //1.1  准备 环境

    val topic = "ods_base_province"
    val groupId = "province_info_group"

    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("ProvinceInfoApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))



    //1.2  用MyKafkaUtil  根据  偏移量  读取

    //1.2.1  根据 topic  和  groupId获取 偏移量

    val offsetsMap = OffsetManagerUtil.getOffset(topic, groupId)

    //  根据偏移量的  获取
    var provinceInfoInputStream : InputDStream[ConsumerRecord[String, String]] = null
    if (offsetsMap != null && offsetsMap.size > 0){
      provinceInfoInputStream  = MyKafkaUtil.getKafkaStream(topic, ssc, offsetsMap, groupId)
    }else{
      provinceInfoInputStream  = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //  保存  已读数据  偏移量  等待后续处理完成  提交  偏移量 到  redis
    //  由于  流 只能流一次  所以 不能直接 用上面的 流直接拿 offsetRangs ;因为这样的话  流 用掉 就不能返回流了  所以 这里 用transform 转换算子
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val provinceInfoDStream = provinceInfoInputStream.transform {
      provinceInfoRDD => {
        offsetRanges = provinceInfoRDD.asInstanceOf[HasOffsetRanges].offsetRanges
        provinceInfoRDD
      }
    }


    //1.3  处理数据:  将 数据 用phoenix  存到  Hbase ;
    // SparkStreaming => Phoenix  用 Phoenix 提供的  saveToPhoenix  方法  ;  处理所有数据 用  foreach  按分区的话 用partition

    provinceInfoDStream.foreachRDD{
      rdd => {

        // 以RDD 为单位 :   先转换
        val provinceInfoRDD = rdd.map {
          provinceInfoRecord => {
            //将 数据 转换成  ProvinceInfo 对象
            JSON.parseObject(provinceInfoRecord.value(), classOf[ProvinceInfo])
          }
        }

        // 再 发送 :    用Phoenix 提供的 saveToPhoenix 方法
        import org.apache.phoenix.spark._
        provinceInfoRDD.saveToPhoenix(
          "GMALL0523_PROVINCE_INFO",
          Seq("ID","NAME","AREA_CODE","ISO_CODE"),
          new Configuration,
          Some("hadoop102,hadoop103hadoop104:2181")
        )

        //  数据保存之后  用  OffsetManagerUtil  提交 偏移量

        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }


    ssc.start()
    ssc.awaitTermination()

  }
}