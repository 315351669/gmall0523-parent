package com.atguigu.gmall.realtime.dwd

import java.util.Date

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.{OrderInfo, ProvinceInfo, UserInfo, UserStatus}
import com.atguigu.gmall.realtime.utils._
import com.ibm.icu.text.SimpleDateFormat
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

//首单指标
object OrderInfoApp {

  def main(args: Array[String]): Unit = {


    //dim 层  *********************************************************************************************


    //1.1  从kafka的gmall0523_db_maxwell主题 中  读取订单信息

    val topic = "ods_order_info"

    val sparkConf = new SparkConf().setAppName("OrderInfoApp").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val groupId = "order_info_group"

    val offsetsMap = OffsetManagerUtil.getOffset(topic, groupId)

    var consumerRecordInputDStream: InputDStream[ConsumerRecord[String, String]] = null

    if (offsetsMap != null && offsetsMap.size>0) {

      consumerRecordInputDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetsMap, groupId)

    } else {
      consumerRecordInputDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }





    //-----------获取偏移量

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

    val consumerRecordDStream = consumerRecordInputDStream.transform {
      consumerRecordInputRDD => {
        offsetRanges = consumerRecordInputRDD.asInstanceOf[HasOffsetRanges].offsetRanges
        consumerRecordInputRDD
      }
    }

    //-----------获取偏移量

    //1.2    判断  订单是否为首单

    //  1.2.1  对流中数据的结构  转换为OrderInfo对象
    val orderInfoDStream = consumerRecordDStream.map {
      //流里是OrderInfo表  对应的  topic  数据  一条记录
      consumerRecord => {
        //kafka topic  的 value 的值  就是  一条orderInfo 记录
        val orderInfoJsonString = consumerRecord.value()
        //将  一条记录(Json)  转换为  OrderInfo  样例类 对象
        val orderInfoJSONObject = JSON.parseObject(orderInfoJsonString, classOf[OrderInfo])


        //给OrderInfo样例类  date  和 hour 属性  赋值
        //从OrderInfo 样例类对象中 取出 create_time  来获取  create_date  和  create_hour    xxxx-xx-xx xx:xx:xx
        val dateSplitedArray = orderInfoJSONObject.create_time.split(" ")
        //将 create_date  赋值         xxxx-xx-xx
        orderInfoJSONObject.create_date = dateSplitedArray(0)
        //将 create_hour  赋值         xx:xx:xx
        orderInfoJSONObject.create_hour = dateSplitedArray(1).split(":")(0)

        //返回OrderInfo样例类对象   的流
        orderInfoJSONObject
      }
    }


    // 1.2.2  根据  在hbase中的订单信息 匹配是否是  首单
    //  拿到 流 中的 所有(每一个 订单id)
    val orderInfoWithFirstDStream = orderInfoDStream.mapPartitions {
      orderInfoIterator => {
        val orderInfoList: List[OrderInfo] = orderInfoIterator.toList

        val userIdList = orderInfoList.map(_.user_id)

        //根据订单id去  hbase  查询  订单首单  状态
        // 拼接sql
        var sql: String = s"select user_id,if_consumed from user_status0523 where user_id in ('${userIdList.mkString("','")}')"

        // 用 工具类的 查询方法 进行 查询   得到  所有  订单 对应的  用户(表里存的都是  非首单 用户  )  下过单用户
        val userIsFirstStatusList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val userIdIsFirstList = userIsFirstStatusList.map(_.getString("USER_ID"))

        // 判断   订单对应的 用户  是否在  下过单用户数据list里  如果包含 说明不是首单  状态标记为  非首单 0
        //循环 订单对应的用户id 判断
        for (orderInfo <- orderInfoList) {
          //判断 订单对应id 是否为 首单用户
          if (userIdIsFirstList.contains(orderInfo.user_id.toString)) { // 如果是非首单
            //标记为  0
            orderInfo.if_first_order = "0"
          } else {
            orderInfo.if_first_order = "1"
          }
        }
        orderInfoList.toIterator
      }
    }


    //  1.4  一个 可能 出现的bug  如果  一个采集周期内  一个人出现多个订单  如果按照上面的  这个人的第二个订单 也会算成 首单
    //解决办法: 一个周期内  一个人的所有订单 只算 最早的那个 (按用户分组 拿到这个人所有的 订单 然后在按时间 升序  取第一个)
    //因为  要分组排序  所有 需要 K-V  类型的 数据,  K为 user_id  V是 数据本身  所以此处需要 转换结构(map)
    val firstOfOneCycleOrderInfoDStream = orderInfoWithFirstDStream.map(orderInfo => (orderInfo.user_id, orderInfo))
    // 根据 key 分组 ;  根据创建时间  排序    分组后 " DStream[(Long, Iterable[OrderInfo])]"
    val firstOrderInfoDStream = firstOfOneCycleOrderInfoDStream.groupByKey()
      .flatMap {
        //模式匹配
        case (user_id, orderInfoIterable) => {
          //迭代器不能排序   转换成 list  ;  根据  创建时间 排序  并且  取出第一个
          orderInfoIterable.toList.sortBy(_.create_time).take(1)
        }
      }


    //dim 层  *********************************************************************************************

    //  2.1    按采集周期的 数据 来处理   |||  关联  hbase 中查出的数据  省份id 进行关联
    //  hbase 查到的数据  转换成 (province_id,provinceInfo)   挨个拿orderInfo中的 provinceId当成key 去获取  对应的 provinceInfo
    val orderWithProvinceDStream = firstOrderInfoDStream.transform {
      orderInfoRDD => {
        //后面遍历orderInfo的时候 需要提前准备好Province数据   //此处需要所有数据  不进行过滤
        val provinceInfoList: List[JSONObject] = PhoenixUtil.queryList("select id,name,area_code,iso_code from gmall0523_province_info")
        //后面需要 根据key来查  所以 需要先将list 转换成 k_V类型
        val provinceInfoMap: Map[String, ProvinceInfo] = provinceInfoList.map {
          provinceInfo => {
            //  *************
            //  (provinceInfo.getString("id"),JSON.parseObject(provinceInfo.toString(),classOf[ProvinceInfo]))

            val province = JSON.toJavaObject(provinceInfo, classOf[ProvinceInfo])
            (province.id, province)

          }
        }.toMap

        val broadcastOfProvinceInfoMap: Broadcast[Map[String, ProvinceInfo]] = ssc.sparkContext.broadcast(provinceInfoMap)

        orderInfoRDD.map {
          orderInfo => {
            val provinceInfo = broadcastOfProvinceInfoMap.value.getOrElse(orderInfo.province_id.toString, null)
            if (provinceInfo != null) {
              orderInfo.province_name = provinceInfo.name
              orderInfo.province_area_code = provinceInfo.area_code
              orderInfo.province_iso_code = provinceInfo.iso_code
            }
            orderInfo
          }
        }
      }
    }



    //  2.2    按采集周期的 数据 来处理   |||  关联  hbase 中查出的数据  用户 id 进行关联
    //要关联的话 需要 order 里的 用户 id (kafka)  和  Hbase  里的 用户 id
//    val orderProvinceAndUser = orderWithProvinceDStream.mapPartitions {
//      orderInfoRDD => {
//        //准备 hbase 的 用户 信息
//        val userIdList = orderInfoRDD.toList.map( _.user_id)
//
//        val sql = s"select id,user_level,birthday,gender,age_group,gender_name from gmall0523_user_info where id in ('${userIdList.mkString("','")}')"
//        val userInfoList: List[JSONObject] = PhoenixUtil.queryList(sql)
//        //因为要用 用户id  匹配 所以  map  结构 比较好  :  将 list  转换为 map  结构
//        val userInfoMapInHbase: Map[String, UserInfo] = userInfoList.map {
//          userInfo => {
//            val userIdMapUserInfo = JSON.toJavaObject(userInfo, classOf[UserInfo])
//            (userIdMapUserInfo.id, userIdMapUserInfo)
//          }
//        }.toMap
//
//        val orderRDD = orderInfoRDD.map {
//          //根据 order 里的 用户 id  去 所有的 用户 id 里找 :  所以 需要  先  准备好  hbase的  用户id
//          orderInfo => {
//            val userInfo = userInfoMapInHbase.getOrElse(orderInfo.user_id.toString, null)
//            if (userInfo != null) {
//              orderInfo.user_age_group = userInfo.age_group
//              orderInfo.user_gender = userInfo.gender_name
//            }
//          }
//            orderInfo
//        }
//        orderRDD
//      }
//
//    }


    val orderProvinceAndUser: DStream[OrderInfo] = orderWithProvinceDStream.mapPartitions {
      orderInfoItr => {
        //转换为list集合
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        //获取所有的用户id
        val userIdList: List[Long] = orderInfoList.map(_.user_id)
        //根据id拼接sql语句，到phoenix查询用户
        var sql: String = s"select id,user_level,birthday,gender,age_group,gender_name from gmall0523_user_info where id in ('${userIdList.mkString("','")}')"
        //当前分区中所有的下单用户
        val userList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val userMap: Map[String, UserInfo] = userList.map {
          userJsonObj => {
            val userInfo: UserInfo = JSON.toJavaObject(userJsonObj, classOf[UserInfo])
            (userInfo.id, userInfo)
          }
        }.toMap
        for (orderInfo <- orderInfoList) {
          val userInfoObj: UserInfo = userMap.getOrElse(orderInfo.user_id.toString, null)
          if (userInfoObj != null) {
            orderInfo.user_age_group = userInfoObj.age_group
            orderInfo.user_gender = userInfoObj.gender_name
          }
        }

        orderInfoList.toIterator
      }
    }

//    orderProvinceAndUser.print(1000)

    //1.3  将  用户 首单状态 信息 维护到  hbase

    orderProvinceAndUser.foreachRDD {
      orderInfoWithFirstRDD => {

        orderInfoWithFirstRDD.cache()


        //只需要将  新增的首单 用户 维护到 hbase 就可以  所以 可以把不需要维护的 用户过滤掉
        val orderInfoOfFirst = orderInfoWithFirstRDD.filter(_.if_first_order.equals("1"))
        //将 orderInfoOfFirst 首单用户 保存到 hbase  (hbase中 用户  是否首单状态 变为 1)
        val userStatus = orderInfoOfFirst.map {
          orderInfo => {
            //将 用户首单状态信息 封装成样例类 对象
            UserStatus(orderInfo.user_id.toString, "1")
          }
        }

        //用 phoenix 的 保存功能
        import org.apache.phoenix.spark._
        //此处为 hadoop 配置文件  因为 hbase 底层存在dfs 里
        userStatus.saveToPhoenix("USER_STATUS0523", Seq("USER_ID", "IF_CONSUMED"), new Configuration, Some("hadoop102,hadoop103,hadoop104:2181"))

        //--------------3.2将订单信息写入到ES中-----------------
        orderInfoWithFirstRDD.foreachPartition {
          orderInfoItr =>{
            val orderInfoList: List[(String,OrderInfo)] = orderInfoItr.toList.map(orderInfo => (orderInfo.id.toString,orderInfo))

            val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
            MyESUtil.bulkInsert(orderInfoList, "gmall0523_order_info_" + dateStr)

            //3.2将订单信息推回 kafka 进入下一层处理   主题： dwd_order_info
            for ((id,orderInfo) <- orderInfoList) {
              //fastjson 要把scala对象包括caseclass转json字符串 需要加入,new SerializeConfig(true)
              MyKafkaSink.sendDStreamToKafkaTopic("dwd_order_info", JSON.toJSONString(orderInfo,new SerializeConfig(true)))
            }

          }
        }


        //保存 偏移量
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
      }
    }


    ssc.start()
    ssc.awaitTermination()

  }

}