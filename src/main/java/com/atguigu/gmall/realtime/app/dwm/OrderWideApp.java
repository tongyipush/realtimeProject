package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.bean.OrderDetail;
import com.atguigu.gmall.realtime.bean.OrderInfo;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;


import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * 需要启动的：
 *      zk
 *      kafka
 *      maxwell
 *      hdfs
 *      hbase
 *      BaseDBApp.java
 *      OrderWideApp.java
 * 执行流程：
 *      -运行生成数据的jar包
 *      -Maxwell采集数据发送到ods_base_db_m
 *      -BaseDBAPP.java从ods层读取数据
 *          *从配置表中读取数据
 *          *将数据进行缓存到hashmap中
 *          *分流：事实数据=> kafka dwd层，维度数据=> hbase dwd
 *      -BaseWideAPP将数据从dwd层读出，组成宽表放到DWM层
 *          *kafka:读取order_info,order_detail
 *          *hbase:读取维度数据
 *
 *
 *
 * 事实数据（绿色）进入kafka数据流（DWD层）中，维度数据（蓝色）进入hbase中长期保存。
 * 那么我们在DWM层中要把实时和维度数据进行整合关联在一起，形成宽表
 *
 *事实数据和事实数据关联，其实就是流与流之间的关联。
 *事实数据与维度数据关联，其实就是流计算中查询外部数据源。
 */
public class OrderWideApp {
    public static void main(String[] args) throws Exception {

        //todo 1.0创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 1.1设置任务并行数量、kafka四个分区，设置4个并行任务数量
        env.setParallelism(4);
//        //todo 2.0设置检查点
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        //todo 2.1设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9820/gmall0921/flink/checkpoint"));
//        //todo 2.2设置重启策略
//        env.setRestartStrategy(RestartStrategies.noRestart());
//        env.getCheckpointConfig().setCheckpointTimeout(60000);

        //todo 从Kafka的ods层读取数据
        String dwd_order_infoTopic = "dwd_order_info";
        String dwd_order_detailTopic = "dwd_order_detail";
        String dwm_order_wideTopic = "dwm_order_wide";
        String dwm_consumer_group_id ="dwm_consumer_order_wide";

        //todo 从Kafka中dwm层读取order_info数据
        FlinkKafkaConsumer<String> dwd_order_info_kafka_source = MyKafkaUtil.getKafkaSource(dwd_order_infoTopic, dwm_consumer_group_id);
        DataStreamSource<String> order_info_kafka_source = (DataStreamSource<String>) env.addSource(dwd_order_info_kafka_source);

        //todo 从Kafka中dwd层读取order_detail数据
        FlinkKafkaConsumer<String> dwd_order_detail_kafka_source = MyKafkaUtil.getKafkaSource(dwd_order_detailTopic, dwm_consumer_group_id);
        DataStreamSource<String> order_detail_kafka_source = env.addSource(dwd_order_detail_kafka_source);

        //todo 将数据进行类型转换、分发水位线
        final SingleOutputStreamOperator<OrderInfo> orderInfoWithWaterMark = order_info_kafka_source.map(
                new RichMapFunction<String, OrderInfo>() {
                    SimpleDateFormat sdf;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderInfo map(String jsonStr) throws Exception {
                        OrderInfo orderInfo = JSON.parseObject(jsonStr, OrderInfo.class);
                        String create_time = orderInfo.getCreate_time();
                        orderInfo.setCreate_ts(sdf.parse(create_time).getTime());
                        return orderInfo;
                    }
                }
        )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                                    @Override
                                    public long extractTimestamp(OrderInfo orderInfo, long recordTimestamp) {
                                        return orderInfo.getCreate_ts();
                                    }
                                })
                );
        SingleOutputStreamOperator<OrderDetail> orderDetailWithWaterMark = order_detail_kafka_source.map(
                new RichMapFunction<String, OrderDetail>() {
                    SimpleDateFormat sdf;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderDetail map(String jsonStr) throws Exception {
                        OrderDetail orderDetail = JSON.parseObject(jsonStr, OrderDetail.class);
                        String create_time = orderDetail.getCreate_time();
                        orderDetail.setCreate_ts(sdf.parse(create_time).getTime());
                        return orderDetail;
                    }
                }
        )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                                    @Override
                                    public long extractTimestamp(OrderDetail orderDetail, long recordTimestamp) {
                                        return orderDetail.getCreate_ts();
                                    }
                                })
                );

        orderDetailWithWaterMark.print("order_detail>>");
        orderInfoWithWaterMark.print("order_info>>");

        //todo 将订单表和事实表进行关联，双流join
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoWithWaterMark.keyBy(r -> r.getId())
                .intervalJoin(orderDetailWithWaterMark.keyBy(r -> r.getOrder_id()))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {

                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });
//        orderWideDS.print(">>>>");


        //todo 用户维度关联
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream
                .unorderedWait(orderWideDS,
                        new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {

                            @Override
                            public String getKey(OrderWide orderWide) {
                                Long key = orderWide.getUser_id();
                                return key.toString();
                            }

                            @Override
                            public void join(OrderWide orderWide, JSONObject jsonObj) throws Exception {

                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                                String birthday = jsonObj.getString("BIRTHDAY");
                                Date date = sdf.parse(birthday);

                                Long curTs = System.currentTimeMillis();
                                Long betweenMs = curTs - date.getTime();
                                Long ageLong = betweenMs / 1000L / 60L / 60L / 24L / 365L;
                                Integer age = ageLong.intValue();
//                                System.out.println("测试年龄"+age);
//                                System.out.println("测试性别"+jsonObj.getString("GENDER"));
                                orderWide.setUser_age(age);
                                orderWide.setUser_gender(jsonObj.getString("GENDER"));
                            }
                        },
                        60,
                        TimeUnit.SECONDS);

//        orderWideWithUserDS.print("用户维度数据宽表>>>");

        //todo 省份维度关联
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(
                orderWideWithUserDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObj) throws Exception {
                        orderWide.setProvince_name(jsonObj.getString("NAME"));
                        orderWide.setProvince_3166_2_code(jsonObj.getString("ISO_3166_2"));
                        orderWide.setProvince_iso_code(jsonObj.getString("ISO_CODE"));
                        orderWide.setProvince_area_code(jsonObj.getString("AREA_CODE"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //TODO 7.关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                        orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                        orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }
                }, 60, TimeUnit.SECONDS);


        //TODO 8.关联SPU商品维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithSkuDS, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);


        //TODO 9.关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 10.关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithCategory3DS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);

        orderWideWithTmDS.print("关联宽表>>>");

        //todo 向Kafka中写入数据，主题dwm_order_wide;
        String dwm_order_wide_topic = "dwm_order_wide";
        FlinkKafkaProducer<String> dwm_order_wide_Sink = MyKafkaUtil.getFlinkSink(dwm_order_wide_topic);
        orderWideWithTmDS
                .map(r -> JSON.toJSONString(r))
                .addSink(dwm_order_wide_Sink);


        //todo 执行
        env.execute();

    }
}
