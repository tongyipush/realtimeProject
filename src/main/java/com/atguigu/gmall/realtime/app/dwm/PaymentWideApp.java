package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.bean.PaymentInfo;
import com.atguigu.gmall.realtime.bean.PaymentWide;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

/**
 *
 * 支付宽表处理主程序
 */
public class PaymentWideApp {

    public static void main(String[] args) throws Exception {

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

//        //todo 设置检查点、检查点超时时间、设置状态后端、设置检查点重启策略
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(50000);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9820/gmall0921/flink/checkpoint"));
//        env.setRestartStrategy(RestartStrategies.noRestart());

        //todo 从dwd层读取payment_info数据
        String source_dwd_payment_info ="dwd_payment_info";
        String source_payment_wide_groupId = "payment_wide_groupId";
        FlinkKafkaConsumer<String> kafkaSource_dwd_payment_info = MyKafkaUtil.getKafkaSource(source_dwd_payment_info, source_payment_wide_groupId);
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDSWithWaterMark = env
                .addSource(kafkaSource_dwd_payment_info)
                .map(r -> JSON.parseObject(r, PaymentInfo.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                                    @Override
                                    public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                                        return DateTimeUtil.toTs(element.getCallback_time());
                                    }
                                })
                );
        //todo 从dwm层读取order_wide数据
        String source_dwm_order_wide = "dwm_order_wide";
        FlinkKafkaConsumer<String> kafkaSource_dwm_order_wide = MyKafkaUtil.getKafkaSource(source_dwm_order_wide, source_payment_wide_groupId);
        SingleOutputStreamOperator<OrderWide> orderWideDSWithWaterMark =
                env
                .addSource(kafkaSource_dwm_order_wide)

                .map(obj -> JSON.parseObject(obj,OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide orderWide, long recordTimestamp) {

                                return DateTimeUtil.toTs( orderWide.getCreate_time() );
                            }
                        })
                );


        //todo 双流join
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDSWithWaterMark.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideDSWithWaterMark.keyBy(OrderWide::getOrder_id))
                .between(Time.seconds(-1800), Time.seconds(0))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) throws Exception {

                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });
        paymentWideDS.print("支付宽表>>>");
        //todo 向Kafka中写入数据
        String dwm_payment_wide ="dwm_payment_wide";
        FlinkKafkaProducer<String> sink_dwm_payment_wide = MyKafkaUtil.getFlinkSink(dwm_payment_wide);

        paymentWideDS
                .map(r -> JSON.toJSONString(r))
                .addSink(sink_dwm_payment_wide);
        //todo 执行
        env.execute();
    }

}
