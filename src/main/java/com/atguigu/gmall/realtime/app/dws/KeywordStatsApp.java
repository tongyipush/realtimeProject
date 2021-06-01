package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.app.func.KeywordUDTF;
import com.atguigu.gmall.realtime.bean.KeywordStats;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 *
 * 搜索关键字计算
 */
public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

//        //todo 设置检查点
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.setRestartStrategy(RestartStrategies.noRestart());
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9820/gmall0921/flink/checkpoint"));
//        env.getCheckpointConfig().setCheckpointTimeout(60000);

        //todo 定义表环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //todo 注册自定义函数
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);
        //todo 读取数据源
        String source_Topic = "dwd_page_log";
        String keyword_Group = "keyword_Group";


        //todo 创建动态表
        // 计算列的定义<computed_column_definition>:
        // 把计算TO_TIMESTAMP(FROM_UNIXTIME(ts/1000))得到的值作为rowtime这列
        // 严格递增时间戳： WATERMARK FOR rowtime_column AS rowtime_column。
        // 有界乱序时间戳： WATERMARK FOR rowtime_column AS rowtime_column - INTERVAL 'string' timeUnit。

        tableEnv.executeSql("CREATE TABLE page_view (" +
                "  common MAP<STRING, STRING>," +
                "  page MAP<STRING, STRING>," +
                "  ts BIGINT," +
                "  rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000))," +
                "  WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND" +
                ") WITH ("+ MyKafkaUtil.getKafkaDDL(source_Topic,keyword_Group) +")");

        //todo 从动态表中将搜索数据查询出来
        // "common":
        //		{"ar":"230000","ba":"Xiaomi","ch":"xiaomi","is_new":"1","md":"Xiaomi Mix2 ","mid":"mid_12","os":"Android 11.0","uid":"28","vc":"v2.1.134"},
        //	"page":
        //		{"during_time":14016,"item":"图书","item_type":"keyword","last_page_id":"home","page_id":"good_list"},
        //		"ts":1616574820000
        // 排除搜索框为空的情况page['item'] IS NOT NULL
        Table fullWordView = tableEnv.sqlQuery("select page['item'] fullword,rowtime from page_view " +
                "where page['page_id']='good_list'"
                + " and page['item'] IS NOT NULL"
        );

        //todo 对搜索中的keyword进行分词
        Table keyWordView = tableEnv.sqlQuery("select keyword, rowtime" +
                " from " + fullWordView + " , LATERAL TABLE(ik_analyze(fullword)) as T(keyword)");

        //todo 分组 开窗 聚合
        Table keyWordStatsSearch = tableEnv.sqlQuery("select keyword,count(*) ct, '"
                + GmallConstant.KEYWORD_SEARCH + "' source ," +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
                "UNIX_TIMESTAMP()*1000 ts from   " + keyWordView
                + " GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND ),keyword");

        //todo 将表转换为流
        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(keyWordStatsSearch, KeywordStats.class);

        //todo 将聚合结果写入click house中
        keywordStatsDataStream.print("keyword>>>");
        keywordStatsDataStream.addSink(ClickHouseUtil.getSinkFunction("insert into keyword_stats_2021(keyword,ct,source,stt,edt,ts) " +
                " values(?,?,?,?,?,?)"));

        env.execute();

    }
}
