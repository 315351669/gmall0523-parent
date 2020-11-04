package com.atguigu.gmall.publisher.controller;

import com.atguigu.gmall.publisher.service.MySQLService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

// 对接DataV的Controller
@RestController
public class DataVController {

    @Autowired
    MySQLService mySQLService;

    @GetMapping("/trademark-sum")
    public Object trademarkSum(@RequestParam("start_date") String startDate,
                               @RequestParam("end_date") String endDate,
                               @RequestParam("topN") int topN) {

        return mySQLService.getTrademardStat(startDate, endDate,topN);
    }


    }
