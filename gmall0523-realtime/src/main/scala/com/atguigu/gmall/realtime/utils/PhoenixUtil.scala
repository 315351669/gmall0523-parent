package com.atguigu.gmall.realtime.utils

import java.sql._

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

object PhoenixUtil{

  def   queryList(sql:String):List[JSONObject]= {

    // 加载 phoenix 驱动
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")

    //phoenix 协议 获取 与hbase的 链接
    val connection = DriverManager.getConnection("jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181")

    //创建一个 Statement  用来执行 sql 语句
    val statement = connection.createStatement()

    //参数 为  输入的  参数

    //用statment执行sql语句
    val resultSet = statement.executeQuery(sql)

    //*****************************

    //处理查询返回的结果集
    val resultSetMetaData = resultSet.getMetaData //结果集中 包含所有表信息

    // 处理所有的结果

    //定义一个集合 来接收  所有  结果集  转成的 JSONObject
    var resultList = new ListBuffer[JSONObject]

    //如果结果集中  有下一个  继续  就一直循环
    while (resultSet.next()) {
      //定义一个jsonObject来接收数据  循环一次  封装一个 (所以在循环内)
      val jsonObject = new JSONObject()

      //根据元数据中列的数量来 遍历 所有列的信息   并将  信息装载到  jsonObject 中
      for (i <- 1 to resultSetMetaData.getColumnCount) {

        //封装每一条  数据 为一个  JSONObeject

        jsonObject.put(resultSetMetaData.getColumnName(i), resultSet.getObject(i))
      }
      //定义一个集合 来接收  所有  结果集  转成的 JSONObject  (在 循环 外面)
      resultList.append(jsonObject)


    }
    //释放资源
    statement.close()
    connection.close()

    //返回查询结果的集合
    resultList.toList

  }






}
