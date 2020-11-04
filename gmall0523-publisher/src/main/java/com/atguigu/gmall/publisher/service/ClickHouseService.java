package com.atguigu.gmall.publisher.service;

import java.math.BigDecimal;
import java.util.Map;

//ClickHourse相关相关的业务接口
public interface ClickHouseService {
    //获取指定日期总的交易额
    BigDecimal getOrderAmountTocal(String date);

    //获取指定日期的分时交易额

    Map<String,BigDecimal> getOrderAmountHour(String date);
}
