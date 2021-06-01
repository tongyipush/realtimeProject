package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.utils.MySQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.*;
import java.util.*;

public class TableProcessFunction extends ProcessFunction<JSONObject,JSONObject> {

    //todo 构造
    OutputTag<JSONObject> outputTag;

    public TableProcessFunction(OutputTag<JSONObject> outputTag) {
        this.outputTag = outputTag;
    }

    //todo open初始
    // 1.读取mysql数据，周期性更新refreshMeta()
    // 2.写入缓存 HashSet<>()
    // 使用hashMap保存表
    // 3.检查建表

    Connection conn = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        //todo 初始化Phoenix连接
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        //todo 最开始时调用，保证数据的及时性
        refreshMeta();
        //todo 周期性更新数据
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                refreshMeta();
            }
        },5000,5000);
    }

    //todo 已经创建过的表放在内存中,存放已经处理过的维度表
    Set<String> set = new HashSet<String>();
    //todo 将数据放在hashMap中，用于存放配置表信息
    Map<String,TableProcess> tableProcessMap = new HashMap<String, TableProcess>();

    //todo 加载配置表信息
    private void refreshMeta(){
        System.out.println("==========加载MySQL配置表信息=============");
        String sql = "select * from table_process";
        List<TableProcess> tableProcessList = MySQLUtil.queryList(sql, TableProcess.class, true);

        //todo 对集合进行遍历
        for (TableProcess tableProcess : tableProcessList) {
            String sourceTable = tableProcess.getSourceTable();
            String operateType = tableProcess.getOperateType();
            String sinkType = tableProcess.getSinkType();
            String sinkTable = tableProcess.getSinkTable();
            String sinkColumns = tableProcess.getSinkColumns();
            String sinkPk = tableProcess.getSinkPk();
            String sinkExtend = tableProcess.getSinkExtend();
            String key = sourceTable+":"+operateType;

            //todo 将读取到的信息放到map集合里边
            tableProcessMap.put(key,tableProcess);

            //todo 判断是否是维度表，检查是否再Hbase中创建过，如果没有那么通过建表语句，将维度创建出来
            if (TableProcess.SINK_TYPE_HBASE.equals(sinkType) && operateType.equals("insert")){
                boolean notExit = set.add(sourceTable);
                if (notExit){
                    //todo 拼接sql，通过phoenix建表
                    checkTable(sinkTable,sinkColumns,sinkPk,sinkExtend);
                }

            }

        }
        if (tableProcessMap == null || tableProcessMap.size()==0){
            throw new RuntimeException("缺少配置信息！！");
        }
    }

    //todo 拼接sql，通过Phoenix创建表
    private void checkTable(String tableName,String fields,String pk,String ext){

        if (pk == null){
            pk = "id";
        }
        if (ext == null){
            ext="";
        }

        String[] fieldsArr = fields.split(",");

        //todo 拼接建表语句
        StringBuilder createSql = new StringBuilder("create table if not exists "+ GmallConfig.HBASE_SCHEMA+
                "."+tableName+"(");
        for (int i = 0; i < fieldsArr.length; i++) {
            String field = fieldsArr[i];
            if (pk.equals(field)){
                //todo 主键对应phoenixrowkey
                createSql.append(field).append(" varchar primary key ");
            }else{
                createSql.append("info.").append(field).append(" varchar ");
            }

            //todo 如果不是最后一个字段使用逗号拼接
            if (i < fieldsArr.length-1){
                createSql.append(",");
            }
        }
        createSql.append(")");
        createSql.append(ext);
//        System.out.println("Phoenix的建表语句是"+createSql);

        PreparedStatement ps =null;
        try {
            ps = conn.prepareStatement(createSql.toString());
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    //todo 核心业务分流， 维度数据到侧输出流，事实数据到主流中
    @Override
    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {

        //todo 获取流中数据的表名
        String tableName = jsonObj.getString("table");
        String type = jsonObj.getString("type");

        //todo 获取data
        // 品牌
        //   {"database":"gmall0921","table":"base_trademark","type":"insert","ts":1616552251,"xid":5068,
        //   "commit":true,"data":{"id":12,"tm_name":"atguigu","logo_url":"/static/beijing.jpg"}}
        // 订单
        //    {"database":"gmall0921","table":"order_info","type":"update","ts":1616552447,"xid":6097,
        //    "commit":true,"data":{"id":1,"consignee":"尤宏","consignee_tel":"13802238806","total_amount":24610.00,
        //    "order_status":"1001","user_id":8,"payment_way":null,"delivery_address":"第12大街第22号楼8单元777门",
        //    "order_comment":"描述239199","out_trade_no":"982347115685883",
        //    "trade_body":"Apple iPhone 12 (A2404) 64GB 蓝色 支持移动联通电信5G 双卡双待手机等3件商品",
        //    "create_time":"2021-03-24 10:20:19","operate_time":"2021-03-24 10:20:19",
        //    "expire_time":"2021-03-24 10:35:19","process_status":null,"tracking_no":null,
        //    "parent_order_id":null,"img_url":"http://img.gmall.com/374359.jpg","province_id":6,"activity_reduce_amount":0.00,"coupon_reduce_amount":0.00,"original_total_amount":24591.00,"feight_fee":19.00,"feight_fee_reduce":null,"refundable_time":null},"old":{"delivery_address":"第12大街第18号楼8单元777门"}}
        //
        JSONObject dataJsonObj = jsonObj.getJSONObject("data");
        //todo maxwell读取历史数据bootstrap
        // 类型修复：如果使用Maxwell导入历史数据，类型为bootstrap-insert,更为insert
        if (type.equals("bootstrap-insert")){
            type = "insert";
            jsonObj.put("type",type);
        }
        //todo 拼接key
        String key = tableName + ":" + type;

        TableProcess tableProcess = tableProcessMap.get(key);

        if (tableProcess!=null){
            jsonObj.put("sink_table",tableProcess.getSinkTable());

            //todo 根据配置表中sink_Column，
            // 对json中的data进行过滤
            filterColumn(dataJsonObj,tableProcess.getSinkColumns());

            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){
                ctx.output(outputTag,jsonObj);
            }else if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())){
                out.collect(jsonObj);
            }

        }else {
            System.out.println("配置表中不存在"+key);
        }
    }

    //todo 对data进行过滤
    private void filterColumn(JSONObject dataJsonObj,String sinkColumns){

        String[] fieldArr = sinkColumns.split(",");
        List<String> fieldList = Arrays.asList(fieldArr);

        Set<Map.Entry<String, Object>> entries = dataJsonObj.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            if (!fieldList.contains(entry.getKey())){
                iterator.remove();
            }
        }
    }

}
