package com.atguigu.gmall.realtime.ods

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.utils.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

//maxwell
object BaseDBMaxwellApp {

  //中心思路:   从kafka  topic读取数据  经过处理  再放回kafka  topic
  def main(args: Array[String]): Unit = {

    //准备1 参数


    //1.1  topic
    val topic = "gmall0523_db_maxwell"

    //1.2  ssc
    val sparkConf = new SparkConf().setAppName("BaseDBCanalApp").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //1.3  groupId    因为1.4中 要用到 所以 先获取
    val groupId = "base_db_maxwell_group"

    //1.4  offsets
    val offsetsMap = OffsetManagerUtil.getOffset(topic,groupId)


      //判断一下   redis 中 offset 是否存在  不存在的话 需要  从  kafka  中 获取
      var consumerRecordInputDStream: InputDStream[ConsumerRecord[String, String]] = null
      if (offsetsMap!=null && offsetsMap.size >0){
        //1.读取kafka topic数据:     参数(topic ,ssc ,groupId,offsets)
        consumerRecordInputDStream = MyKafkaUtil.getKafkaStream(topic,ssc,offsetsMap,groupId)
      }else{
        consumerRecordInputDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
      }




    var offsetRanges : Array[OffsetRange] = Array.empty[OffsetRange]
    val consumerRecordDStream = consumerRecordInputDStream.transform {
      consumerRecordInputRDD => {
        offsetRanges = consumerRecordInputRDD.asInstanceOf[HasOffsetRanges].offsetRanges
        consumerRecordInputRDD
      }
    }

//    consumerRecordDStream.print(1000)

    //2.处理数据  并  放回kafka  topic            //  分流

    consumerRecordDStream
      //对数据的结构进行转换  先将 json 转换成  jsonObject
      .map{
      consumerRecord => {
        val consumerRecordJson = consumerRecord.value()
        val consumerRecordJsonObject = JSON.parseObject(consumerRecordJson)
        consumerRecordJsonObject
      }
    }
      //根据json对象中 属性(表名)  将数据发送至不同的  kafka  topic
        .foreachRDD{
          consumerRecordRDD => {
            consumerRecordRDD.foreach{
              consumerRecordJsonObject => {
                val opType = consumerRecordJsonObject.getString("type")
                val dataJsonObject = consumerRecordJsonObject.getJSONObject("data")
                val tableName = consumerRecordJsonObject.getString("table")

                if(dataJsonObject!=null && !dataJsonObject.isEmpty ){
                  if(
                    ("order_info".equals(tableName)&&"insert".equals(opType))
                      || (tableName.equals("order_detail") && "insert".equals(opType))
                      ||  tableName.equals("base_province")
                      ||  tableName.equals("user_info")
                      ||  tableName.equals("sku_info")
                      ||  tableName.equals("base_trademark")
                      ||  tableName.equals("base_category3")
                      ||  tableName.equals("spu_info")
                  ){
                    //拼接要发送到的主题
                    val sendTopic = "ods_" + tableName
                    MyKafkaSink.sendDStreamToKafkaTopic(sendTopic,dataJsonObject.toString)
                  }
                }
              }
            }


            //3.更新 redis中 偏移量
            OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
          }
        }





    ssc.start()
    ssc.awaitTermination()

  }

}
