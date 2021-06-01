package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 用户跳出率的条件是
 *  1.last_page_id为空
 *      (1).出，跳转 => 判断页面是否有问题
 *      (2).跳出 => 进入页面直接×掉了 => 和推广有关系，判断在那个网站做推广有价值
 *  2.一段时间内：比如10s内没有本站其他页面的访问
 *      CEP匹配10s内有uer访问的放在主流中，10内没有访问的放在侧输出流中，即超时时间处理流中
 *
 */

public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        String topic ="dwd_page_log";
        String groupId = "UserJumpDetailGroup";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);

        DataStreamSource<String> kafkaDSStr = env.addSource(kafkaSource);

//        DataStreamSource<String> kafkaDSStr = env.fromElements(
//                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
//                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
//                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                        "\"home\"},\"ts\":15000} ",
//                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                        "\"detail\"},\"ts\":30000} "
//        );


        //todo 格式转换 string => JSONObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDSStr.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String jsonStr) throws Exception {
                        return JSON.parseObject(jsonStr);
                    }
                }
        );

        //todo 指定时间语义以及时间时间字段watermark，从flink1.2开始，默认的时间为事件时间
        SingleOutputStreamOperator<JSONObject> jsonObjWithWaterMark = jsonObjDS.assignTimestampsAndWatermarks(

                WatermarkStrategy
                        //todo todo 单调递增，
                        .<JSONObject>forMonotonousTimestamps()
                        //todo 乱序
//                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                return jsonObj.getLong("ts");
                            }
                        })
        );

        //todo 分流
        KeyedStream<JSONObject, String> keyedDS = jsonObjWithWaterMark.keyBy(r -> r.getJSONObject("common").getString("mid"));

        //todo cep模式匹配
        Pattern<JSONObject, JSONObject> pattern = Pattern
                .<JSONObject>begin("first")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        //todo 判断是否为首次访问
                        // 如果上一页id不为空，那么该用户为从app其他页面跳转本页面，不算跳出
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        if (lastPageId == null || lastPageId.length() == 0) {
                            return true;
                        }
                        return false;
                    }
                })
                .next("second")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        //todo 如果10s内没有第二条数据，那么second不会触发
                        // 只有在10s内该用户在该app访问其他用户的时候，second程序才会触发
                        String pageId = jsonObj.getJSONObject("page").getString("page_id");
                        if (pageId != null && pageId.length() > 0){

                            return true;
                        }
                        return false;
                    }
                })
                .within(Time.seconds(10));

        //todo 处理超时匹配数据
        // 如果10s内第一个数据last_page_id为空，并且10s内第二个数据的page_id不为空，匹配成功
        // 10s内该用户没有在该APP的页面出现，那么认为跳出

        //todo 加{},因为使用匿名内部类
        OutputTag<String> outputTag = new OutputTag<String>("side_output"){};
        final SingleOutputStreamOperator<Object> realDS = CEP.pattern(keyedDS, pattern)
                .flatSelect(
                        outputTag,
                        new PatternFlatTimeoutFunction<JSONObject, String>() {
                            @Override
                            public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<String> out) throws Exception {
                                JSONObject jsonObj = pattern.get("first").get(0);
                                out.collect(jsonObj.toString());
                            }
                        }, new PatternFlatSelectFunction<JSONObject, Object>() {
                            @Override
                            public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<Object> out) throws Exception {
                                //todo 完全匹配，10s内有访问本app的第二个页面，
                                // 不属于当前访问范围
                            }
                        }

                );

        DataStream<String> timeOutDS = realDS.getSideOutput(outputTag);

        timeOutDS.print();

        //todo 将跳转用户明细，写到Kafka的dwm层

        String dwm_user_jump = "dwm_user_jump_detail";
        timeOutDS.addSink(MyKafkaUtil.getFlinkSink(dwm_user_jump));


        //todo 执行
        env.execute();


    }
}
