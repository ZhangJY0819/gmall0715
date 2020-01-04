package com.atguigu.gmall0715.publisher.service.impl;

import com.atguigu.gmall0715.publisher.mapper.DauMapper;
import com.atguigu.gmall0715.publisher.mapper.OrderMapper;
import com.atguigu.gmall0715.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImp implements PublisherService {

    @Autowired
    DauMapper dauMapper;
    @Autowired
    OrderMapper orderMapper;

    @Override
    public Long getDauCount(String date) {
        return dauMapper.selectDauCount(date);
    }

    @Override
    public Map getDauCountHour(String date) {

        Map resultMap= new HashMap<>();

        List<Map> mapList = dauMapper.selectDauCountHour(date);

        for (Map map : mapList) {
            resultMap.put(map.get("LOGHOUR"), map.get("CT"));
        }
        return resultMap;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.selectOrderAmount(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {
        List<Map> mapList = orderMapper.selectOrderAmountHour(date);
        Map resultMap = new HashMap();
        for (Map map : mapList) {
            resultMap.put(map.get("CREATE_HOUR"), map.get("ORDER_AMOUNT"));
        }
        return resultMap;
    }
}
