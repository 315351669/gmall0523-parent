package com.atguigu.gmall.publisher.service;


import java.util.List;
import java.util.Map;

//从ads层中获取数据提供的服务接口
public interface MySQLService {

    List<Map> getTrademardStat(String startDate, String endDate, int topN);

}
