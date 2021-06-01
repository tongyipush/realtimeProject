package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * 对phonexUtil进行封装,只需要输入表名，字段名就可以查询一行结果
 *
 */
public class DimUtil {

    public static JSONObject getDimInfo(String tableName,String id){

        JSONObject id1 = getDimInfo(tableName, Tuple2.of("id", id));
        return id1;
    }

    public static JSONObject getDimInfo(String tableName,Tuple2<String,String>...columns){



        //todo 获取redis的key值
        StringBuilder keyStr = new StringBuilder();
        //todo key
        // dim:user:100_zs
        // dim:user:200_ls
        keyStr.append("dim:"+tableName+":");

        StringBuilder sqlStr = new StringBuilder();

        sqlStr.append("select * from "+ tableName +" where ");
        for (int i = 0; i < columns.length; i++) {

            Tuple2<String, String> column = columns[i];
            String field = column.f0;
            String value = column.f1;
            if (i>0){
                sqlStr.append("and ");
                keyStr.append("_");
            }
            sqlStr.append(field +"= '"+ value+"'");
            keyStr.append(value);
        }

        Jedis jedis = RedisUtil.getJedis();
        String jsonObjStr = jedis.get(keyStr.toString());
        JSONObject jsonObject = null;

        if (jsonObjStr == null || jsonObjStr.length() ==0){
            System.out.println("在Phoenix中查询数据："+sqlStr);
            List<JSONObject> jsonObjects = PhoenixUtil.queryList(sqlStr.toString(), JSONObject.class);
            if ( jsonObjects!=null && jsonObjects.size()>0){
                jsonObject = jsonObjects.get(0);
                if (jedis != null ){
                    jedis.setex(keyStr.toString(),24*60*60,jsonObjects.get(0).toString());
                }
            }else {
                System.out.println("维度数据没有找到！");
            }
        }else{
             jsonObject = JSON.parseObject(jsonObjStr);
        }

        //todo 关闭jedis连接
        if (jedis != null){
            jedis.close();
            System.out.println("关闭缓存连接！");
        }
        return jsonObject;
    }

    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String,String>...columns){

        StringBuilder sqlStr = new StringBuilder();

        sqlStr.append("select * from "+ tableName +" where ");
        for (int i = 0; i < columns.length; i++) {

            Tuple2<String, String> column = columns[i];
            String field = column.f0;
            String value = column.f1;
            if (i>0){
                sqlStr.append("and ");
            }
            sqlStr.append(field +"= '"+ value+"'");
        }
        System.out.println("在Phoenix中查询数据："+sqlStr);
        List<JSONObject> jsonObjects = PhoenixUtil.queryList(sqlStr.toString(), JSONObject.class);

        JSONObject jsonObject = null;
        if (jsonObjects !=null && jsonObjects.size()>0){
            jsonObject = jsonObjects.get(0);
        }else{
            System.out.println("维度数据没有找到！");
        }
        return jsonObject;

    }

    //todo 根据key让jedis中的缓存失效
    public static void deleteCached(String tableName,String id){

        try {
            //todo 根据连接池，获取连接
            Jedis jedis = RedisUtil.getJedis();
            String key = "dim:"+tableName+":"+id;
            //todo 通过key清除缓存
            jedis.del(key);
            jedis.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("缓存异常！");
        }
    }

    //todo 测试
    public static void main(String[] args) {
//        JSONObject dimInfoNoCache = getDimInfoNoCache("dim_base_trademark", Tuple2.of("id", "13"),Tuple2.of("TM_NAME","zhuyuchen"));
//        System.out.println(dimInfoNoCache);

        JSONObject dim_base_trademark = getDimInfo("dim_base_trademark", "13");
        System.out.println(dim_base_trademark);
    }
}
