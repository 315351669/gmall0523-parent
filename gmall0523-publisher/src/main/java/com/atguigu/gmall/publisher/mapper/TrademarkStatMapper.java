package com.atguigu.gmall.publisher.mapper;

import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;


public interface TrademarkStatMapper {
    /**
     * @Author
     * @Description
     * @Date 11/3/2020 7:28 PM
     * @Param [startDate, endDate, topN]
     * @return java.util.List<java.util.Map>
     */
    List<Map> selectTradeSum(@Param("start_time") String startDate ,
                                    @Param("end_time")String endDate,
                                    @Param("topN")int topN);
}
