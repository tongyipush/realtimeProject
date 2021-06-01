package com.atguigu.gmall.realtime.utils;

import com.atguigu.gmall.realtime.bean.TableProcess;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MySQLUtil {

    //todo 从MySQL表查询数据，将查寻的结果封装为相应的类型
    public  static <T> List<T> queryList(String sql,Class<T> clazz,boolean underSourceToCamel){

        ResultSet rs=null;
        PreparedStatement ps=null;
        Connection conn=null;
        try {
            //注册驱动

            Class.forName("com.mysql.jdbc.Driver");
            //获取连接
            conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall_realtime?characterEncoding=utf-8&useSSL=false"
                    , "root",
                    "123456"
            );
            //获取数据库操作对象
            ps = conn.prepareStatement(sql);
            //执行sql语句
            rs = ps.executeQuery();
            //处理结果集
            ResultSetMetaData md = rs.getMetaData();
            //声明集合对象，用来封装返回结果
            List<T> resList = new ArrayList<>();

            //todo 将查询结果一行一行的遍历
            while (rs.next()){

                //todo 通过反射，获取实例
                T obj = clazz.newInstance();
                //todo mysql结果集遍历从1开始
                for (int i = 1; i <= md.getColumnCount(); i++) {

                    //todo 获取列名
                    // 列的下表是从1开始
                    String properName = md.getColumnName(i);
                    //todo 将下划线转换为驼峰命名法 guava
                    if (underSourceToCamel){
                        //todo 修改properName
                        properName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, properName);
                    }
                    //todo 使用beanUtils工具类给obj对象的属性赋值
                    BeanUtils.setProperty(obj,properName,rs.getObject(i));
                }
                resList.add(obj);

            }
            //todo 返回结果
            return resList;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("查询mysql失败!");
        }finally {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }

            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }

            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
    }

    public static void main(String[] args) {
        String sql = "select * from table_process";
        List<TableProcess> result = queryList(sql, TableProcess.class, true);
        System.out.println(result.get(1));
    }
    }
