package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.DimUtil;
import com.atguigu.gmall.realtime.utils.ThreadPoolUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 *
 * 异步向hbase中查询数据async
 *
 */

//todo 从官网查询异步IO需要继承的方法
    //todo 声明泛型模板<T>  extends RichAsyncFunction<T,T>
public abstract class DimAsyncFunction<T>  extends RichAsyncFunction<T,T> implements DimJoinFunction<T>{

    //todo 多态的引用
    private ExecutorService executorService;
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        executorService = ThreadPoolUtil.getInstance();
    }

    //todo 提供异步操作的方法
    // 从线程池里边拿取多个值
    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {

        executorService.submit(new Runnable() {
            @Override
            public void run() {

                try {
                    long start = System.currentTimeMillis();
                    //todo 获取维度表的key
                    String key = getKey(obj);
                    //todo 从维度表中获取维度数据
                    JSONObject dimInfo = DimUtil.getDimInfo(tableName, key);
                    if (dimInfo != null && dimInfo.size()>0){

                        //todo 事实数据和维度数据进行关联
                        join(obj,dimInfo);
                    }
                    long end = System.currentTimeMillis();
                    System.out.println("异步维度关联耗时:"+(end-start)+"毫秒");

                } catch (Exception e) {
                    System.out.println(tableName+"维度查询异常！");
                    e.printStackTrace();
                }
                //todo 将关联之后的结果发送到下游
                resultFuture.complete(Arrays.asList(obj));
            }
        });



    }
}
