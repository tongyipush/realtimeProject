package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.VisitorStats;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;


public class VisitorStatsApp {
    public static <T> void main(String[] args) throws Exception {

        //todo 基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 设置并行度
        env.setParallelism(4);

//        //todo 设置检查点
//        // 检查点保存策略，间隔时间
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        //检查点保存路径
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9820/gmall0921/flink/checkpoint"))
//        //连接超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        //重启策略
//        env.setRestartStrategy(RestartStrategies.noRestart());

        //todo 从Kafka的pv,uv,跳转明细中读取数据
        String pv_topic = "dwd_page_log";
        String uv_topic = "dwm_unique_visit";
        String jump_detail_topic = "dwm_user_jump_detail";
        String visitStats_consumer_groupId = "visitStats_consumer_groupId";

        //todo 从dwd层读取page_log
        FlinkKafkaConsumer<String> kafka_source_dwd_page_log = MyKafkaUtil.getKafkaSource(pv_topic, visitStats_consumer_groupId);
        SingleOutputStreamOperator<VisitorStats> page_detail_visitorStatsDSWithWaterMark = env
                .addSource(kafka_source_dwd_page_log)
                .map(new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);

                        VisitorStats visitorStats = new VisitorStats(
                                "",
                                "",
                                jsonObj.getJSONObject("common").getString("vc"),
                                jsonObj.getJSONObject("common").getString("ch"),
                                jsonObj.getJSONObject("common").getString("ar"),
                                jsonObj.getJSONObject("common").getString("is_new"),
                                0L,
                                1L,
                                0L,
                                0L,
                                jsonObj.getJSONObject("page").getLong("during_time"),
                                jsonObj.getLong("ts")
                        );
                        String last_Page_Id = jsonObj.getJSONObject("page").getString("last_page_id");
                        //todo &&=> 如果last_Page_Id == null，使用&&那么last_Page_Id.length()空指针
                        if ( last_Page_Id == null || last_Page_Id.length() == 0) {
                            visitorStats.setSv_ct(1L);
                        }

                        return visitorStats;
                    }
                });
        //todo 从dwm层读取unique_uv
        FlinkKafkaConsumer<String> kafka_source_unique_visit = MyKafkaUtil.getKafkaSource(uv_topic, visitStats_consumer_groupId);

        SingleOutputStreamOperator<VisitorStats> unique_visit_visitorStatsDS = env
                .addSource(kafka_source_unique_visit)
                .map(new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        VisitorStats visitorStats = new VisitorStats(
                                "",
                                "",
                                jsonObj.getJSONObject("common").getString("vc"),
                                jsonObj.getJSONObject("common").getString("ch"),
                                jsonObj.getJSONObject("common").getString("ar"),
                                jsonObj.getJSONObject("common").getString("is_new"),
                                1L,
                                0L,
                                0L,
                                0L,
                                0L,
                                jsonObj.getLong("ts")
                        );
                        return visitorStats;
                    }
                });

        //todo 从dwm层读取jump_detail
        FlinkKafkaConsumer<String> kafka_source_jump_detail = MyKafkaUtil.getKafkaSource(jump_detail_topic, visitStats_consumer_groupId);

        SingleOutputStreamOperator<VisitorStats> jump_detail_visitorStatsDS = env
                .addSource(kafka_source_jump_detail)
                .map(new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        VisitorStats visitorStats = new VisitorStats(
                                "",
                                "",
                                jsonObj.getJSONObject("common").getString("vc"),
                                jsonObj.getJSONObject("common").getString("ch"),
                                jsonObj.getJSONObject("common").getString("ar"),
                                jsonObj.getJSONObject("common").getString("is_new"),
                                0L,
                                0L,
                                0L,
                                1L,
                                0L,
                                jsonObj.getLong("ts")
                        );
                        return visitorStats;
                    }
                });


        //todo 对以上三条流进行合并,按照渠道、地区、版本、新老用户维度进行分组
        SingleOutputStreamOperator<VisitorStats> windowDS = page_detail_visitorStatsDSWithWaterMark
                .union(unique_visit_visitorStatsDS, jump_detail_visitorStatsDS)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<VisitorStats>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                                    @Override
                                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                })

                )
                .keyBy(new KeySelector<VisitorStats, Tuple4<String,String,String,String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(VisitorStats r) throws Exception {
                        return Tuple4.of(r.getCh(),r.getAr(),r.getVc(),r.getIs_new());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats visitorStats1, VisitorStats visitorStats2) throws Exception {
                        //todo 会话
                        visitorStats1.setSv_ct(visitorStats1.getSv_ct() + visitorStats2.getSv_ct());
                        //todo 独立访客
                        visitorStats1.setUv_ct(visitorStats1.getUv_ct() + visitorStats2.getUv_ct());
                        //todo page访问数
                        visitorStats1.setPv_ct(visitorStats1.getPv_ct() + visitorStats2.getPv_ct());
                        //todo 跳转数
                        visitorStats1.setUj_ct(visitorStats1.getUj_ct() + visitorStats2.getUj_ct());
                        //todo 页面停留时间
                        visitorStats1.setDur_sum(visitorStats1.getDur_sum() + visitorStats2.getDur_sum());

                        return visitorStats1;
                    }
                }, new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void process(Tuple4<String, String, String, String> key, Context context, Iterable<VisitorStats> elements, Collector<VisitorStats> out) throws Exception {

                        long startLong = context.window().getStart();
                        String start = DateTimeUtil.toYMDhms(new Date(startLong));

                        long endLong = context.window().getEnd();
                        String end = DateTimeUtil.toYMDhms(new Date(endLong));

                        for (VisitorStats visitorStats : elements) {

                            visitorStats.setStt(start);
                            visitorStats.setEdt(end);
                            out.collect(visitorStats);
                        }
                    }
                });

//        windowDS.print("<<<<");

//        SinkFunction<VisitorStats> sink = JdbcSink.sink(" insert into visitor_stats_0921 values(?,?,?,?,?,?,?,?,?,?,?,?)",
//                new JdbcStatementBuilder<VisitorStats>() {
//                    @Override
//                    public void accept(PreparedStatement preparedStatement, VisitorStats visitorStats) throws SQLException {
//
//                    }
//                },
//                new JdbcExecutionOptions.Builder().withBatchSize(5).build(),
//                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
//                        .withUrl(GmallConfig.CLICKHOUSE_URL)
//                        .build()
//        );

        //todo 向clickHouse中写入数据
        windowDS.addSink(ClickHouseUtil.getSinkFunction("insert into visitor_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        env.execute();
    }
}
