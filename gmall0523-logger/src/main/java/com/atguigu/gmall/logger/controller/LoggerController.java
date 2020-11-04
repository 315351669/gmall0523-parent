package com.atguigu.gmall.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


//标识为controller组件，交给Sprint容器管理，并接收处理请求  如果返回String，会当作网页进行跳转
   
// @RestController = @Controller + @ResponseBody  会将返回结果转换为json进行响应
@Slf4j    //Lombok插件注解  用来处理日志
@RestController
public class LoggerController {

    //spring boot 提供的kafka支持  直接调用send()方法
    //下面我们是没有自己new来创建对象的  如果需要spring boot的 controller来创建 需要加注解 Autowired( 属性 注入)
    @Autowired
    KafkaTemplate kafkaTemplate;

    //通过requestMapping匹配请求并交给方法处理   可以用GetMapping来替换(get请求)
    @RequestMapping("/applog")
    //@RequestBody注解标记的参数  就是 请求 发过来的参数(此处为: applog 日志)
    public String applog(@RequestBody String mockLog){
        //打印来测试
        //System.out.println(mockLog);

        //处理日志  落盘   (顺便打印)
        log.info(mockLog);

        //把不同类型(启动,事件)的日志  放入kafka不同的主题中
        //字符串转json   从json对象中取出  类型 来判断
        //面向对象编程   需要一个 Json类型的对象来 操作
//{"common":{"ar":"110000","ba":"Xiaomi","ch":"xiaomi","md":"Xiaomi 10 Pro ","mid":"mid_15","os":"Android 10.0","uid":"154","vc":"v2.1.134"},
// "start":{"entry":"notice","loading_time":16148,"open_ad_id":13,"open_ad_ms":9868,"open_ad_skip_ms":0},"ts":1602943112000}
        //把整条日志转成 Json对象
        JSONObject jsonObject = JSON.parseObject(mockLog);
        //然而其   属性(common 和  start/事件)的值 还是一个json对象: 用  getJsonObeject()方法来
        JSONObject valueOfStart = jsonObject.getJSONObject("start");

        if (valueOfStart != null){
            //如果valueOfStart的值不为null,则该日志为 start 日志:  发送至 kafka 启动日志  topic
            // 1. 正常  应该 创建一个kafka 生产者  并 使用 发送方法 :  new KafkaProducer<String,String>().send(new ProducerRecord<>())
            //  但是  spring boot 给我提供了 kafka模板 (KafkaTemplate)类   ****  放在方法外面
            kafkaTemplate.send("gmall_start_0523", mockLog);
            //注意:  自带模板并没有提供  kafka 集群信息,所以 需要在spring boot的配置文件中配置

        }else {
            //如果valueOfStart的值为null,则该日志为 事件 日志:  发送至 kafka 事件日志 topic
            kafkaTemplate.send("gmall_event_0523",mockLog);
        }

        return "success!!!";
    }

}