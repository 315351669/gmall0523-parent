package com.atguigu.gmall.realtime.utils

import java.util.Properties

object MyPropertiesUtil {
  //  def main(args: Array[String]): Unit = {
  //    println(MyPropertiesUtil.load("config.properties").getProperty("kafka.broker.list"))
  //  }

  def load(propertyName: String): Properties = {
    val prop = new Properties()
    prop.load(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertyName))
    prop
  }

}
