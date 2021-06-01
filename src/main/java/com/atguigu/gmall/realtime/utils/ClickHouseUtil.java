package com.atguigu.gmall.realtime.utils;

import com.atguigu.gmall.realtime.bean.TransientSink;
import com.atguigu.gmall.realtime.bean.VisitorStats;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {

    public static <T> SinkFunction<T> getSinkFunction(String sql){


        SinkFunction<T> clickHouseSink = JdbcSink.<T>sink(
                sql,
                //todo 通过数据库操作对象，对sql中的占位符进行赋值
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T obj) {

                        //todo 通过反射获取类的属性
                        Field[] fields = obj.getClass().getDeclaredFields();
                        int jump = 0;
                        //todo 对类中的属性进行遍历
                        for (int i = 0; i < fields.length; i++) {
                            //每遍历一次获取一个属性对象
                            Field field = fields[i];
                            //判断该属性是否需要传到click house中
                            TransientSink annotation = field.getAnnotation(TransientSink.class);
                            if (annotation != null) {
                                jump ++;
                                //跳过当前循环，进入下一个循环
                                continue;
                            }
                            //设置属性访问权限
                            field.setAccessible(true);

                            //获取属性的值
                            try {
                                //获取属性的值
                                Object fieldvalue = field.get(obj);
                                //将属性的值传给占位符
                                preparedStatement.setObject(i + 1 - jump, fieldvalue);
                            } catch (Exception e) {
                                e.printStackTrace();
                                throw new RuntimeException("获取字段值时异常!");
                            }

                        }
                    }
                },
                //执行参数配置，每次执行的数量是3次
                new JdbcExecutionOptions.Builder().withBatchSize(5).build(),
                //连接参数配置：4要素=驱动+URL+用户名+密码
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build()
        );

        return clickHouseSink;
    }
}
