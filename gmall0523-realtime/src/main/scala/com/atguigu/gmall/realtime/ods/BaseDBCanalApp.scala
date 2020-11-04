package com.atguigu.gmall.realtime.ods

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.utils.OffsetManagerUtil
import com.atguigu.gmall.realtime.utils.{MyKafkaSink, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BaseDBCanalApp {

  def main(args: Array[String]): Unit = {


    //1.0.1准备topic参数
    val topic = "gmall0523_db_canal"

    //1.0.2准备ssc参数
    val sparkConf = new SparkConf().setAppName("BaseDBCanalApp").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //1.0.3.1准备offsets参数的参数
    val groupId = "base_db_canal_group"
    //1.0.3  先获取offsets  然后从起始位置 读取数据
    val offsetsMap = OffsetManagerUtil.getOffset(topic,groupId)

    //1.0 用kafka工具类读取 kafka 数据 为DStream
    //**  注意 :   上面获取的 offsets 是否为空  决定  从kafka读取数据的位置  所以 提取 canalKafkaDStream  var
    var consumerRecordInputDStream : InputDStream[ConsumerRecord[String, String]] = null
    if(offsetsMap != null && !offsetsMap.isEmpty){
      //redis 有消费记录  则 从消费记录处 读取数据
      consumerRecordInputDStream = MyKafkaUtil.getKafkaStream(topic,ssc,offsetsMap,groupId)
    }
    else{
      //redis 没有消费记录  则 从头开始读取数据
      consumerRecordInputDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }


    //1.2.1  拿到  读数据的  偏移量   区间  (用于提交这次读到哪个地方)
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val consumerRecordDStream = consumerRecordInputDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }


    //将  流数据  转为 json 对象  的  流
    val jsonObjectDStream = consumerRecordDStream.map {
      //record  ==  DStream[ConsumerRecord[String, String]]
      consumeRecord => {
        val record = consumeRecord.value()
        val recordJsonObject = JSON.parseObject(record)
        recordJsonObject
      }
    }



    //1.1 分流: 用foreachRDD将canal_to_kafka的一个topic sparkstreaming流 根据表名 分成多个 topic(发送到kafka)
    jsonObjectDStream.foreachRDD{
          //这层是jsonObejectRDD(类似集合)  还需要下一层  此处用map和 foreach  应该都可以
      jsonObjectRDD => {
        //对jsonObjectRDD 进行操作  foreach
        jsonObjectRDD.foreach{
              //这层是jsonObject了
          jsonObject => {


            //根据表名 拼成新 topic名称  将 原来的数据 发到新的topic中( 还需要 获取 类型操作)

                //获取操作类型
            val opType = jsonObject.getString("type")
            if ("INSERT".equals(opType)){
              //获取表名
              val tableName = jsonObject.getString("table")
              val tableTopic = "ods_" + tableName
                //获取数据( 此处 需求为 插入数据才算新增 所以 上面可以先判断type 类型为 insert 再执行 否则 没必要)
              val dataArray = jsonObject.getJSONArray("data")

              //此处数据为数组 可能为 批量插入操作  所以 遍历发送
              import scala.collection.JavaConverters._
              for (data <- dataArray.asScala) {
                MyKafkaSink.sendDStreamToKafkaTopic(tableTopic,data.toString)
              }


            }

          }
        }
        //1.2  提交  offsets
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()

  }

}
