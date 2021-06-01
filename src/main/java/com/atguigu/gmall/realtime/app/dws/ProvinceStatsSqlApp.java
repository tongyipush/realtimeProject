package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.bean.ProvinceStats;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //todo 创建表运行时环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //todo 从Kafka数据源中获取数据
        String orderWideTopic = "dwm_order_wide";
        String groupId_province = "province_stats";

        tableEnv.executeSql("CREATE TABLE ORDER_WIDE (" +
                "province_id BIGINT, " +
                "province_name STRING," +
                "province_area_code STRING," +
                "province_iso_code STRING," +
                "province_3166_2_code STRING," +
                "order_id STRING, " +
                "split_total_amount DOUBLE," +
                "create_time STRING," +
                //todo TO_TIMESTAMP函数将字符串转换为一定格式的时间，赋值给rowtime
                "rowtime AS TO_TIMESTAMP(create_time) ," +
                //todo 指定时间时间，并取别名
                "WATERMARK FOR  rowtime  AS rowtime)" +
                " WITH (" + MyKafkaUtil.getKafkaDDL(orderWideTopic, groupId_province) + ")");

        //todo 进行聚合计算
        Table tableResult = tableEnv.sqlQuery("select " +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') stt, " +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') edt , " +
                " province_id," +
                " province_name," +
                "province_area_code area_code," +
                "province_iso_code iso_code ," +
                "province_3166_2_code iso_3166_2 ," +
                "COUNT( DISTINCT  order_id) order_count, " +
                "sum(split_total_amount) order_amount," +
                "UNIX_TIMESTAMP()*1000 ts " +
                " from  ORDER_WIDE " +
                " group by  TUMBLE(rowtime, INTERVAL '10' SECOND )," +
                " province_id,province_name,province_area_code,province_iso_code,province_3166_2_code ");

        //TODO 将表转换为流
        DataStream<ProvinceStats> provinceStatsDataStream = tableEnv.toAppendStream(tableResult, ProvinceStats.class);

        provinceStatsDataStream.print("省份主题测试>>>");
        //todo 将查询结果写入clickHouse

        provinceStatsDataStream.addSink(ClickHouseUtil.getSinkFunction("insert into  province_stats_2021 values(?,?,?,?,?,?,?,?,?,?)"));

        env.execute();
    }
}
