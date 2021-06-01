package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TableProcessFunction_CDC extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    //声明Phoenix的连接对象
    Connection conn = null;
    private OutputTag<JSONObject> outputTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction_CDC(OutputTag<JSONObject> outputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化Phoenix连接
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    private void checkTable(String tableName, String fields, String pk, String ext) {
        if (pk == null) {
            pk = "id";
        }

        if (ext == null) {
            ext = "";
        }

        //拼接建表语句
        StringBuilder createSql = new StringBuilder("create table if not exists " + GmallConfig.HBASE_SCHEMA + "." + tableName + "(");

        String[] fieldArr = fields.split(",");
        for (int i = 0; i < fieldArr.length; i++) {
            String field = fieldArr[i];
            if (pk.equals(field)) {
                createSql.append(field).append(" varchar primary key ");
            } else {
                createSql.append(field).append(" varchar ");
            }
            if (i < fieldArr.length - 1) {
                createSql.append(",");
            }
        }

        createSql.append(")");
        createSql.append(ext);
        System.out.println("创建Phoenix表的语句" + createSql);

        //获取Phoenix连接
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(createSql.toString());
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Phoenix建表失败");
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    @Override
    public void close() throws Exception {
        System.out.println("----close----");
        if (conn != null) {
            conn.close();
        }
    }

    private void filterColumn(JSONObject dataJsonObj, String sinkColumns) {
        String[] cols = sinkColumns.split(",");
        List<String> colList = Arrays.asList(cols);
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();

        Iterator<Map.Entry<String, Object>> it = entrySet.iterator();

        for (; it.hasNext(); ) {
            Map.Entry<String, Object> entry = it.next();
            if (!colList.contains(entry.getKey())) {
                it.remove();
            }
        }
    }

    @Override
    public void processElement(JSONObject jsonObj, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //取出状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        String table = jsonObj.getString("table");
        String type = jsonObj.getString("type");
        JSONObject dataJsonObj = jsonObj.getJSONObject("data");

        if (type.equals("bootstrap-insert")) {
            type = "insert";
            jsonObj.put("type", type);
        }

        //从状态中获取配置信息
        String key = table + ":" + type;
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {
            jsonObj.put("sink_table", tableProcess.getSinkTable());
            if (tableProcess.getSinkColumns() != null && tableProcess.getSinkColumns().length() > 0) {
                filterColumn(dataJsonObj, tableProcess.getSinkColumns());
            }
            if (tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_HBASE)) {
                ctx.output(outputTag, jsonObj);
            } else if (tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_KAFKA)) {
                out.collect(jsonObj);
            }
        } else {
            System.out.println("NO this Key in TableProce" + key);
        }
    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        //将单条数据转换为JSON对象
        JSONObject jsonObject = JSON.parseObject(value);
        //获取其中的data
        String data = jsonObject.getString("data");
        //将data转换为TableProcess对象
        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);

        //获取源表表名
        String sourceTable = tableProcess.getSourceTable();
        //获取操作类型
        String operateType = tableProcess.getOperateType();
        //输出类型      hbase|kafka
        String sinkType = tableProcess.getSinkType();
        //输出目的地表名或者主题名
        String sinkTable = tableProcess.getSinkTable();
        //输出字段
        String sinkColumns = tableProcess.getSinkColumns();
        //表的主键
        String sinkPk = tableProcess.getSinkPk();
        //建表扩展语句
        String sinkExtend = tableProcess.getSinkExtend();
        //拼接保存配置的key
        String key = sourceTable + ":" + operateType;


        //如果是维度数据，需要通过Phoenix创建表
        if (TableProcess.SINK_TYPE_HBASE.equals(sinkType) && "insert".equals(operateType)) {
            checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);
        }

        //获取状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //将数据写入状态进行广播
        broadcastState.put(key, tableProcess);
    }

}
