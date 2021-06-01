package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * dwm层：独立访客unique_visit
 *  1.将数据从dwd_page_log读取
 *  2.将数据进行格式转换 : string => JSONObj
 *  3.对mid进行分流
 *  4.对数据进行过滤
 *      (1)设置状态变量firstVisitDate,保留当天第一次访问的时间
 *      (2)将last_page_id为空的保留
 *  5.将数据写入kafka中dwm层：dwm_unique_visit
 *
 */
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 设置任务并行度，与Kafka分区保持一致
        env.setParallelism(4);
        //todo 从Kafka中读取数据
        String topic = "dwd_page_log";
        String groupId = "uniqueVisitApp_group";

        //todo 从dwd层dwd_page_log读取数据
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kfDS = env.addSource(kafkaSource);

        //todo 对数据类型进行转换 string => JSONObject
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kfDS.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String jsonStr) throws Exception {
                        return JSON.parseObject(jsonStr);
                    }
                }
        );

//        jsonObjDS.print("DWM>>>");

        //todo 按照mid分流
         KeyedStream<JSONObject, String> midKeyDS = jsonObjDS.keyBy(r -> r.getJSONObject("common").getString("mid"));
         //todo 过滤
        SingleOutputStreamOperator<JSONObject> filterDS = midKeyDS.filter(
                new RichFilterFunction<JSONObject>() {
                    //todo 定义状态变量
                    // 首次访问时间
                    ValueState<String> firstVisitState;
                    //todo 定义格式化日期转换工具
                    SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<>("firstVisitState", Types.STRING);
                        //todo 设置状态变量存活时间1天
                        StateTtlConfig stateTtlConfig = StateTtlConfig
                                .newBuilder(Time.days(1))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .build();
                        stringValueStateDescriptor.enableTimeToLive(stateTtlConfig);

                        firstVisitState = getRuntimeContext().getState(stringValueStateDescriptor);

                        sdf = new SimpleDateFormat("yyyyMMdd");
                    }

                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {

                        String lastPageId = jsonObj.getJSONObject("common").getString("last_page_id");
                        //todo 如果上一页的pageid不为空，说明是从app的其他页面跳转过来，
                        // 不是首次进入的第一个页面，需要过滤掉
                        if (lastPageId != null && lastPageId.length() > 0) {

                            return false;
                        }


                        Long ts = jsonObj.getLong("ts");
                        String curDate = sdf.format(ts);
                        //todo 如果firstVisitState不为空，并且与ts时间相同，那么是同一天的数据，需要过滤掉
                        // 如果firstVisitState为空，说名visit是今天第一次访问，更新状态
                        String mid = jsonObj.getJSONObject("common").getString("mid");
                        if (firstVisitState.value() != null && firstVisitState.value().length() > 0 && firstVisitState.value().equals(curDate)) {

                            //todo 访问过，返回false，将结果过滤掉
                            System.out.println("已访问过=>mid"+mid+"firstVisitDate："+firstVisitState.value()
                                        +"curDate："+curDate
                                    );
                            return false;
                        } else {
                            //todo 说明没有访问，那么返回true，保留结果
                            System.out.println("未访问过=>mid"+mid+"firstVisitDate："+firstVisitState.value()
                                    +"curDate："+curDate);
                            firstVisitState.update(curDate);
                            return true;
                        }

                    }
                }
        );

        filterDS.print("日志文件向KafkaDIM层写入数据！");
        //todo 向Kafka主题 中发送数据
        //todo 执行

        String topicDim = "dwm_unique_visit";
        FlinkKafkaProducer<String> unique_visit_sink = MyKafkaUtil.getFlinkSink(topicDim);
        //todo 将JSONObj转换为string的原因
        // MyKafkaUtil.getFlinkSink(topicDim)中写入Kafka主题中的元素是string类型，使类型统一，成功调用
        // filterDSStr<string>.addSink（）
        SingleOutputStreamOperator<String> filterDSStr = filterDS.map(
                new MapFunction<JSONObject, String>() {
                    @Override
                    public String map(JSONObject value) throws Exception {
                        return value.toString();
                    }
                }
        );
        filterDSStr.addSink(unique_visit_sink);
        env.execute();
    }
}
