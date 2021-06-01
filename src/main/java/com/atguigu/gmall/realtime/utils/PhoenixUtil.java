package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * (1)MySql和Phneonix都遵循JDBC原则
 * (2)封装MySQL工具类的时候，每查询一次创建一个连接，但是在PhoenixDriver中，一条流过来
 *      可能要查询多条数据，因此需要初始化一个连接
 *
 *
 */
public class PhoenixUtil {

   private static Connection conn;
   //todo 初始化连接
   private  static void init()  {
           try {
               //todo 注册驱动
               Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
               //todo 使用驱动建立连接
               conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
               //todo 确定hbase 的哪个命名空间里边
               conn.setSchema(GmallConfig.HBASE_SCHEMA);

           } catch (Exception e) {
               e.printStackTrace();
           }
   }

   //todo 提供phonenix查询的方法，返回执行的封装的对象
   public static <T>List<T> queryList(String sql,Class<T> clz){
       if (conn == null){
           init();
       }

       //todo 初始化赋值
       ResultSet rs = null ;
       PreparedStatement ps = null;
       ArrayList<T> list = new ArrayList<>();
       try {
           //todo 创建数据库操作对象
           ps = conn.prepareStatement(sql);
           //todo 执行查询，获得结果集
           rs = ps.executeQuery();
           ResultSetMetaData metaData = rs.getMetaData();
           //todo 通过反射获取类
           T t = clz.newInstance();

           //todo 对每一行数据进行封装
           while (rs.next()){
               //todo 列名下标从1开始
               for (int i = 1; i <= metaData.getColumnCount(); i++) {
                   String columnName = metaData.getColumnName(i);
                   BeanUtils.setProperty(t,columnName,rs.getObject(i));
               }
               list.add(t);
           }

       } catch (Exception e) {
           e.printStackTrace();
           throw new RuntimeException("在phoenix中查询数据失败！");
       }finally {
           //todo 关闭资源
           if (rs != null){
               try {
                   rs.close();
               } catch (SQLException e) {
                   e.printStackTrace();
               }
           }

           if (ps != null){
               try {
                   ps.close();
               } catch (SQLException e) {
                   e.printStackTrace();
               }
           }
       }

        return list;
   }

    public static void main(String[] args) {

       String sql = "select * from dim_base_trademark";
        List<JSONObject> jsonObjects = queryList(sql, JSONObject.class);
        System.out.println(jsonObjects);
    }

}
