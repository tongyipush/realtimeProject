package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

/**
 *
 * 维度关联查询接口
 * @param <T>
 */

public interface DimJoinFunction<T>{
    String getKey(T key);
    void join(T obj, JSONObject jsonObj) throws Exception;
}
