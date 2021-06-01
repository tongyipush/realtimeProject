package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * 向Phoenix中写入数据
 * (1)初始化连接
 * (2)
 */
public class DimSink extends RichSinkFunction<JSONObject> {


    Connection conn;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //todo 1.初始化连接
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject jsonObj, Context context)  {

        //todo 获取目标维度表
        String tableName = jsonObj.getString("sink_table");
        //todo 获取data数据
        JSONObject dataJsonObj = jsonObj.getJSONObject("data");

        if ( dataJsonObj!=null && dataJsonObj.size()>0 ) {

            String upsertSql = genUpsertSql(tableName, dataJsonObj);
            PreparedStatement ps = null;
            try {
                //todo 生成upsert语句
                System.out.println("向Phoenix中插入数据sql语句："+upsertSql);

                //todo 创建数据库操作对象
                ps = conn.prepareStatement(upsertSql);
                // todo 执行
                ps.executeUpdate();
                //todo mysql自动提交事务 ctrl+h connection =>
                // 查看private boolean isAutoCommit = false;
                // pheonix是手动提交事务
                conn.commit();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException("执行sql失败");
            }finally {
                if (ps!=null){
                    try {
                        ps.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        //todo 删除redis缓存
        if (jsonObj.getString("type").equals("update") || jsonObj.getString("type").equals("delete") ){

            DimUtil.deleteCached(tableName.toLowerCase(),dataJsonObj.getString("id"));
        }
    }

    public String genUpsertSql(String tableName,JSONObject dataJSONObj){

        //todo 获取所有的key
        Set<String> fields = dataJSONObj.keySet();
        //todo 获取所有的value
        Collection<Object> values = dataJSONObj.values();
        //todo 工具类StringUtils.join，
        // 将集合的元素之间插入符号
        String upsertSql = "upsert into "+GmallConfig.HBASE_SCHEMA+"."+tableName+"("+ StringUtils.join(fields,",")
                +") values "+"( '"+StringUtils.join(values,"','")+"')";

        return upsertSql;
    }
}
