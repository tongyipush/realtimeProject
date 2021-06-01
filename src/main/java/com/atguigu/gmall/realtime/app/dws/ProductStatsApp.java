package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.bean.PaymentWide;
import com.atguigu.gmall.realtime.bean.ProductStats;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import scala.reflect.internal.Trees;

import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * Author: Felix
 * Desc: 形成以商品为准的统计  曝光 点击  购物车  下单 支付  退单  评论数 宽表
 */

public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        
        //todo 基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);
//        //todo 设置检查点
//        //检查点保存策略，保存间隔时间
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        //检查点保存路径
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9820/gmall0921/flink/checkpoint"));
//        //重启策略
//        env.setRestartStrategy(RestartStrategies.noRestart());
//        //连接超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(50000);

        //todo 从Kafka中获取数据流

        //TODO 1.从Kafka中获取数据流
        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";
        FlinkKafkaConsumer<String> pageViewSource  = MyKafkaUtil.getKafkaSource(pageViewSourceTopic,groupId);
        FlinkKafkaConsumer<String> orderWideSource  = MyKafkaUtil.getKafkaSource(orderWideSourceTopic,groupId);
        FlinkKafkaConsumer<String> paymentWideSource  = MyKafkaUtil.getKafkaSource(paymentWideSourceTopic,groupId);
        FlinkKafkaConsumer<String> favorInfoSourceSouce  = MyKafkaUtil.getKafkaSource(favorInfoSourceTopic,groupId);
        FlinkKafkaConsumer<String> cartInfoSource  = MyKafkaUtil.getKafkaSource(cartInfoSourceTopic,groupId);
        FlinkKafkaConsumer<String> refundInfoSource  = MyKafkaUtil.getKafkaSource(refundInfoSourceTopic,groupId);
        FlinkKafkaConsumer<String> commentInfoSource  = MyKafkaUtil.getKafkaSource(commentInfoSourceTopic,groupId);

        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> favorInfoDStream = env.addSource(favorInfoSourceSouce);
        DataStreamSource<String> orderWideDStream= env.addSource(orderWideSource);
        DataStreamSource<String> paymentWideDStream= env.addSource(paymentWideSource);
        DataStreamSource<String> cartInfoDStream= env.addSource(cartInfoSource);
        DataStreamSource<String> refundInfoDStream= env.addSource(refundInfoSource);
        DataStreamSource<String> commentInfoDStream= env.addSource(commentInfoSource);

        //todo 对获取的数据流进行结构的转换
        // 3.1转换曝光页面流数据

        SingleOutputStreamOperator<ProductStats> display_pageClick_ProductStats = pageViewDStream.process(new ProcessFunction<String, ProductStats>() {
            @Override
            public void processElement(String jsonStr, Context ctx, Collector<ProductStats> out) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                JSONObject pageObj = jsonObj.getJSONObject("page");
                //判断商品详情页面点击情况
                if ("good_detail".equals(pageObj.getString("page_id"))) {
                    Long sku_id = pageObj.getLong("item");
                    Long ts = jsonObj.getLong("ts");
                    ProductStats click_productStats = ProductStats.builder().sku_id(sku_id)
                            .click_ct(1L).ts(ts).build();
                    out.collect(click_productStats);
                }


                //判断曝光页面情况
                JSONArray displays = jsonObj.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {

                    for (int i = 0; i < displays.size(); i++) {
                        //jsonArr用的是getJSONObject(i),而数组用的是array[i]
                        JSONObject display = displays.getJSONObject(i);
                        //曝光的可能是商品，也可能是活动，所以需要判断一下
                        if ("sku_id".equals(display.getString("item_type"))) {
                            Long sku_id = display.getLong("item");
                            Long ts = jsonObj.getLong("ts");
                            ProductStats display_productStats = ProductStats.builder().sku_id(sku_id)
                                    .display_ct(1L).ts(ts).build();
                            out.collect(display_productStats);
                        }
                    }
                }
            }
        });

        //todo 3.2转换下单流数据
        SingleOutputStreamOperator<ProductStats> orderWide_ProductStats = orderWideDStream.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        OrderWide orderWideObj = JSON.parseObject(jsonStr, OrderWide.class);
                        String create_time = orderWideObj.getCreate_time();
                        Long ts = DateTimeUtil.toTs(create_time);
                        return ProductStats.builder()
                                .sku_id(orderWideObj.getSku_id())
                                .order_amount(orderWideObj.getSplit_total_amount())
                                .order_sku_num(orderWideObj.getSku_num())
                                .orderIdSet(new HashSet(Collections.singleton(orderWideObj.getOrder_id())))
                                .ts(ts)
                                .build();
                    }
                }
        );

        //todo 转换收藏数据流
        SingleOutputStreamOperator<ProductStats> favor_ProductStats = favorInfoDStream.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        JSONObject favorObj = JSON.parseObject(jsonStr);
                        Long ts = DateTimeUtil.toTs(favorObj.getString("create_time"));
                        return ProductStats.builder()
                                .sku_id(favorObj.getLong("sku_id"))
                                .favor_ct(1L)
                                .ts(ts)
                                .build();
                    }
                }
        );
        //todo 转换购物车流数据
        SingleOutputStreamOperator<ProductStats> cart_ProductStats = cartInfoDStream.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        JSONObject cartJsonObj = JSON.parseObject(jsonStr);
                        Long ts = DateTimeUtil.toTs(cartJsonObj.getString("create_time"));
                        return ProductStats.builder()
                                .sku_id(cartJsonObj.getLong("sku_id"))
                                .cart_ct(1L)
                                .ts(ts)
                                .build();
                    }
                }
        );

        //todo 转换支付数据流
        SingleOutputStreamOperator<ProductStats> payment_ProductStats = paymentWideDStream.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {

                        PaymentWide paymentWideObj = JSON.parseObject(jsonStr, PaymentWide.class);
                        Long ts = DateTimeUtil.toTs(paymentWideObj.getPayment_create_time());
                        return ProductStats.builder()
                                .sku_id(paymentWideObj.getSku_id())
                                .payment_amount(paymentWideObj.getSplit_total_amount())
                                .paidOrderIdSet(new HashSet(Collections.singleton(paymentWideObj.getOrder_id())))
                                .ts(ts)
                                .build();
                    }
                }
        );

        //todo 转换退款数据流
        SingleOutputStreamOperator<ProductStats> refund_ProductStats = refundInfoDStream.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsoStr) throws Exception {
                        JSONObject refundObj = JSON.parseObject(jsoStr);
                        Long ts = DateTimeUtil.toTs(refundObj.getString("create_time"));
                        return ProductStats.builder()
                                .sku_id(refundObj.getLong("sku_id"))
                                .refund_amount(refundObj.getBigDecimal("refund_amount"))
                                //一个订单可能有多个退款，需要set去重
                                .refundOrderIdSet(new HashSet(Collections.singleton(refundObj.getLong("order_id"))))
                                .ts(ts)
                                .build();
                    }
                }
        );

        //todo 转换评价数据流
        SingleOutputStreamOperator<ProductStats> comment_ProductStats = commentInfoDStream.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {

                        JSONObject commentObj = JSON.parseObject(jsonStr);
                        Long ts = DateTimeUtil.toTs(commentObj.getString("create_time"));

                        Long goodCt = GmallConstant.APPRAISE_GOOD.equals(commentObj.getString("appraise")) ? 1L : 0L;
                        return ProductStats.builder()
                                .sku_id(commentObj.getLong("sku_id"))
                                .comment_ct(1L)
                                .good_comment_ct(goodCt)
                                .ts(ts)
                                .build();
                    }
                }
        );

        //todo 4.对流进行合流
        // 分发水位线，提取时间时间
        SingleOutputStreamOperator<ProductStats> productStatsDSWithWaterMark = display_pageClick_ProductStats.union(
                orderWide_ProductStats,
                favor_ProductStats,
                cart_ProductStats,
                payment_ProductStats,
                refund_ProductStats,
                comment_ProductStats
        )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ProductStats>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                                    @Override
                                    public long extractTimestamp(ProductStats element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                })
                );

        //todo 分流开窗、聚合
        SingleOutputStreamOperator<ProductStats> reduceDS = productStatsDSWithWaterMark.keyBy(new KeySelector<ProductStats, Long>() {
            @Override
            public Long getKey(ProductStats productStats) throws Exception {


                return productStats.getSku_id();
            }
        })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {

                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                        //把集合order_id去重后相加在一起
                        stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                        //.size()是int类型，但是需要Long类型，所以需要加0L
                        stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                        stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                        stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                        stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);

                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());

                        return stats1;

                    }
                }, new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void process(Long aLong, Context context, Iterable<ProductStats> elements, Collector<ProductStats> out) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        ProductStats productStats = elements.iterator().next();
                        productStats.setStt(DateTimeUtil.toYMDhms(new Date(start)));
                        productStats.setEdt(DateTimeUtil.toYMDhms(new Date(end)));

                        //把时间戳更改为本地时间
                        productStats.setTs(new Date().getTime());

                        out.collect(productStats);
                    }
                });

        //todo 对维度进行关联
        // 使用异步查询hbase
        // flink异步查询接口
        //补充sku维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDstream = AsyncDataStream.unorderedWait(reduceDS, new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        Long sku_id = productStats.getSku_id();
                        return sku_id.toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimJsonObj) throws Exception {
                        productStats.setSku_name(dimJsonObj.getString("SKU_NAME"));
                        productStats.setSku_price(dimJsonObj.getBigDecimal("PRICE"));
                        productStats.setCategory3_id(dimJsonObj.getLong("CATEGORY3_ID"));
                        productStats.setSpu_id(dimJsonObj.getLong("SPU_ID"));
                        productStats.setTm_id(dimJsonObj.getLong("TM_ID"));
                    }
                }, 60,
                TimeUnit.SECONDS);

        //补充spu维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDstream = AsyncDataStream.unorderedWait(productStatsWithSkuDstream,
                new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSpu_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject jsonObj) throws Exception {
                        productStats.setSpu_name(jsonObj.getString("SPU_NAME"));
                    }
                }, 60,
                TimeUnit.SECONDS);

        //补充品类维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3Dstream = AsyncDataStream.unorderedWait(
                productStatsWithSpuDstream,
                new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return String.valueOf(productStats.getCategory3_id());
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject jsonObj) throws Exception {

                        productStats.setCategory3_name(jsonObj.getString("NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //补充品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDstream = AsyncDataStream.unorderedWait(
                productStatsWithCategory3Dstream,
                new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return String.valueOf(productStats.getTm_id());
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject jsonObj) throws Exception {
                        productStats.setTm_name(jsonObj.getString("TM_NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //测试
        productStatsWithTmDstream.print("20210404");

        //todo 将数据写入click house
        productStatsWithTmDstream.addSink(
        ClickHouseUtil.getSinkFunction("insert into product_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        );

        env.execute();


    }
}
