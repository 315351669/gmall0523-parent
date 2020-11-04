package com.atguigu.gmall.realtime.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object MyRedisUtil {
  def main(args: Array[String]): Unit = {

    //    //直接new jedis 只需要host 和 端口号就可以  从配置文件获取即可
    //    //    val jedis = new Jedis("localhost", 6379)
    //    //    println(jedis.ping())
    //    val redis_host = MyPropertiesUtil.load("config.properties").getProperty("redis.host")
    //    val redis_port = MyPropertiesUtil.load("config.properties").getProperty("redis.port")
    //
    //    //        val jedisPool = new JedisPool(redis_host, redis_port.toInt)
    //    //        val jedis = jedisPool.getResource
    //    //        println(jedis.ping())
    //
    //
    //    //JedisPool也可以只用host 和 port 直接获得,但是一般都会修改默认配置
    //    val jedisPoolConfig = new JedisPoolConfig
    //
    //      jedisPoolConfig.setMaxTotal(10)
    //      jedisPoolConfig.setMaxIdle(5)
    //      jedisPoolConfig.setMinIdle(5)
    //      jedisPoolConfig.setBlockWhenExhausted(true)
    //      jedisPoolConfig.setMaxWaitMillis(2000)
    //      jedisPoolConfig.setTestOnBorrow(true)
    //
    //    //根据参数获取JedisPool
    //    val jedisPool = new JedisPool(jedisPoolConfig, redis_host, redis_port.toInt)
    //
    //    //从JedisPool中获取链接
    //    val jedis = jedisPool.getResource
    //    //测试
    //    println(jedis.ping())

//    println(getJedisClient().ping())

  }

  //封装工具方法
  var jedisPool: JedisPool = null

  def build()= {
    val jedisPoolConfig = new JedisPoolConfig

    jedisPoolConfig.setMaxTotal(100) //最大连接数
    jedisPoolConfig.setMaxIdle(20) //最大空闲
    jedisPoolConfig.setMinIdle(20) //最小空闲
    jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
    jedisPoolConfig.setMaxWaitMillis(5000) //忙碌时等待时长 毫秒
    //borrow(借:即拿链接)   还可以设置Create(创建)   Return(返回:断开)   Idle(空闲)
    jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

    val properties = MyPropertiesUtil.load("config.properties")
    val redis_host = properties.getProperty("redis.host", "localhost")
    val redis_port = properties.getProperty("redis.port", "6379")

    jedisPool = new JedisPool(jedisPoolConfig, redis_host, redis_port.toInt)
  }

  def getJedisClient(): Jedis = {

    if (jedisPool==null) {
      build()
    }
    jedisPool.getResource()

  }
}