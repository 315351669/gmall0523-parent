package com.atguigu.gmall.realtime.bean

//日活数据 往ES中保存时  的 样例类对象
//    mid  uid   ar  os  ch  md    vc  ba  dt

//{"common":{"ar","ba","ch","md","mid","os","uid","vc",
case class DauInfo(
                    mid:String,//设备id
                    uid:String,//用户id
                    ar:String,//地区
                    ch:String,//渠道
                    vc:String,//版本
                    var dt:String,//日期
                    var hr:String,//小时
                    var mi:String,//分钟
                    ts:Long //时间戳
                  ) {}

