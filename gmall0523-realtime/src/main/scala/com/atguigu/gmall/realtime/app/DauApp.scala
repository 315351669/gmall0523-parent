package com.atguigu.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.DauInfo
import com.atguigu.gmall.realtime.utils.OffsetManagerUtil
import com.atguigu.gmall.realtime.utils.{MyESUtil, MyKafkaUtil, MyRedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer


object DauApp {
  def main(args: Array[String]): Unit = {

    //  MyKafkaUtil从kafka读数据  DStream  并处理一下时间

    //参数准备  topic  groupId
    val topic = "gmall_start_0523"

    val sparkConf = new SparkConf().setAppName("DauApp").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val groupId = "gmall_dau_0523"

    val kafkaDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)

    //    val jsonDStream = kafkaDStream.map(_.value())
    //    jsonDStream.print(1000)

    //{"common":{"ar":"370000","ba":"Xiaomi","ch":"oppo","md":"Xiaomi 10 Pro ","mid":"mid_26","os":"Android 10.0",
    // "uid":"416","vc":"v2.1.132"},"start":{"entry":"icon","loading_time":4115,"open_ad_id":13,"open_ad_ms":9084,
    // "open_ad_skip_ms":1742},"ts":1603078557000}

    //*********************************
    //从Redis中获取Kafka分区偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)

    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsetMap!=null && offsetMap.size >0){
      //如果Redis中存在当前消费者组对该主题的偏移量信息，那么从执行的偏移量位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,offsetMap,groupId)
    }else{
      //如果Redis中没有当前消费者组对该主题的偏移量信息，那么还是按照配置，从最新位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }

    //获取当前采集周期从Kafka中消费的数据的起始偏移量以及结束偏移量值
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        //因为recodeDStream底层封装的是KafkaRDD，混入了HasOffsetRanges特质，这个特质中提供了可以获取偏移量范围的方法
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }
    //*********************************

    val jsonObjectStream = offsetDStream.map {
      jsonString => {
        val jsonObject = JSON.parseObject(jsonString.value())
        val dateStr = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(jsonObject.getLong("ts")))
        val dateStrArr = dateStr.split(" ")
        jsonObject.put("dt", dateStrArr(0))
        jsonObject.put("hr", dateStrArr(1))
        jsonObject
      }
    }
    //    jsonStream.print(1000)
    //{"dt":"2020-10-22","common":{"ar":"110000","uid":"127","os":"iOS 13.3.1","ch":"Appstore","md":"iPhone X",
    // "mid":"mid_15","vc":"v2.1.134","ba":"iPhone"},"start":{"entry":"icon","open_ad_skip_ms":0,"open_ad_ms":6102,
    // "loading_time":7858,"open_ad_id":16},"hr":"11","ts":1603338865000}



    // 利用 redis 对登陆设备进行 首登 过滤 ;   下一步 将结果存往  ES
    val firstTimeLoginDStream = jsonObjectStream.mapPartitions {

      jsonObjects => {

        //放入redis   根据返回结果做          " 操作 "

        //保存到redis
        //先获取数据 => key: "dau:2020-10-23"   value:  "mid"

        //获取链接 才能操作
        val jedis = MyRedisUtil.getJedisClient()
        //在for循环里用来存新增的活跃设备 之后发往ES保存
        val firstTimeLoginList = new ListBuffer[JSONObject]()

        for (jsonObject <- jsonObjects) {
          val dauKey = "dau:" + jsonObject.getString("dt")
          val dauValue = jsonObject.getJSONObject("common").getString("mid")

          //此处需要jedis客户端发送数据 所以在   "for循环"  前面准备好
          val isFirstTime = jedis.sadd(dauKey, dauValue)

          //  1.根据返回结果  决定是否 保存到集合(可变集合 ListBuffer) 准备放入 ES
          //如果1(首次),则放入集合中保存(需要在for循环外先定义)  并且将key的失效时间设置为1天
          if (isFirstTime == 1L) {
            firstTimeLoginList.append(jsonObject)
            jedis.expire(dauKey, 3600 * 24)
          }
          //  2.返回结果为0  不操作

        }

        jedis.close()

        //将需要发往ES的  首次登陆设备的list返回 (注:  此处需求 可迭代的)
        firstTimeLoginList.toIterator
      }

    }



    //    firstTimeLoginDStream.count().print()
    //  将  每批  新增 日活 保存至  ES

    firstTimeLoginDStream.foreachRDD {
      firstLoginRDD => {
        firstLoginRDD.foreachPartition {
          //RDD内为  可迭代的  首次登陆  集合 (firstLogins)
          firstLogins => {




            //将 可迭代  的  firstLogins  转换为  一条一条的 firstLogin
            val dauInfoList = firstLogins.map {
              //将 一条  首登  数据 (firstLogin) 封装为一个 样例类对象
              firstLogin => {
                val firstLoginData = firstLogin.getJSONObject("common")
                val dauInfo = DauInfo(
                  firstLoginData.getString("mid"),
                  firstLoginData.getString("uid"),
                  firstLoginData.getString("ar"),
                  firstLoginData.getString("ch"),
                  firstLoginData.getString("vc"),
                  firstLoginData.getString("dt"),
                  firstLoginData.getString("hr"),
                  "00",
                  firstLoginData.getLong("ts")
                )
                (dauInfo.mid,dauInfo)
              }
            }.toList

            //此处按照  批量将 数据导入  ES 中 ,  上面返回值默认为  可迭代  . 我们list 操作比较熟练 所以  上面 toList
            val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            MyESUtil.bulkInsert(dauInfoList,"gmall0523_dau_info_" + dt)
          }

        }
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }


    ssc.start()
    ssc.awaitTermination()
  }
}