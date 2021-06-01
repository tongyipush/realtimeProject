package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.Checkpoint;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.table.KafkaSinkSemantic;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 运行进程
 *  zk、kafka、 logger.sh(ngix+采集服务bootstrap) BaseLogApp
 *
 *log日志数据执行流程
 *  1.运行模拟生成日志数据的jar
 *      mock.type: "http"
 *      #http模式下，发送的地址
 *      mock.url: "http://hadoop102:80/applog"
 *
 *  2.数据发给ngix,ngix端口为80，ngix反向代理，负载均衡
 *      location /applog{
 *            proxy_pass http://www.logserver.com;
 *      }
 *      upstream www.logserver.com{
 *         server hadoop102:8081;
 *         server hadoop103:8081;
 *         server hadoop104:8081;
 *     }
 *  3.ngix将数据转发给102，103，103上的日志采集服务bootstrap,bootstrap采集服务器端口:8081
 *      upstream www.logserver.com{
 *         server hadoop102:8081;
 *         server hadoop103:8081;
 *         server hadoop104:8081;
 *     }
 *  4.日志采集服务springboot落盘功能使用logback，并将数据发送到kafka的ods_base_log
 *      /opt/module/rt_gmall/gmall0921-logger-0.0.1-SNAPSHOT.jar
 *  5.BaseLogApp从kafka主题ods_base_log里边读取数据，进行状态修复，分流，根据日志类型发送到DWD层Kafka的不同主题
 *      dwd_page_log、dwd_start_log、dwd_display_log
 *          hbase原因：大数据平台，维度常驻，用户量比较大，放在hbase中更合适点
 *
 */

public class BaseLogApp {
    public static void main(String[] args) throws Exception {

        //todo 创建运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 1.1设置任务并行数量
        env.setParallelism(4);

//        //todo 1.2设置检查点
//        env.enableCheckpointing(5000,  CheckpointingMode.EXACTLY_ONCE);
//        //todo 1.3设置检查点超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        //todo 1.4设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9820/gmall0921/flink/checkpoint"));
//        //todo 1.5指定hdfs操作的用户
//        System.setProperty("HADOOP_USER_NAME","atguigu");
//        //todo 1.5.1 虚拟机运行hdfs dfs -chmod -R 777 /
//        //todo 1.6设置重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,1000));
//        env.setRestartStrategy(RestartStrategies.noRestart());


        //todo 从Kafka中读取数据
        String topic = "ods_base_log";
        String groupId = "ods_base_group";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic,groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

//        kafkaDS.print(">>>");

        //todo 2.4对读取的数据进行转换 jsonString => jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS
                .map(
                        new MapFunction<String, JSONObject>() {
                            @Override
                            public JSONObject map(String jsonStr) throws Exception {
                                return JSON.parseObject(jsonStr);
                            }
                        }
                );

        //todo 3.0新老访客状态修复
        // 3.1根据key进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS
                .keyBy(r -> r.getJSONObject("common").getString("mid"));

        //todo 3.2状态修复
        SingleOutputStreamOperator<JSONObject> midWithNewFlagDS = keyedDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {

                    ValueState<String> firstVisit;
                    SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        firstVisit = getRuntimeContext().getState(new ValueStateDescriptor<String>("firstVisit", Types.STRING));
                        sdf = new SimpleDateFormat("yyyyMMdd");
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObj) throws Exception {
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");
                        Long ts = jsonObj.getLong("ts");

                        //todo 状态是1，可能需要修复
                        if ("1".equals(isNew)) {
                            String curTime = sdf.format(new Date(ts));
                            if (firstVisit.value() != null && firstVisit.value().length() > 0) {

                                if (!firstVisit.value().equals(curTime)) {
                                    String is_new = "0";
                                    jsonObj.getJSONObject("common").put("is_new", is_new);
                                }
                            } else {
                                firstVisit.update(curTime);
                            }
                        }
                        return jsonObj;
                    }
                }

        );

        //todo 分流 启动日志，曝光日志， 页面日志
        // 测输出流的两个作用，处理迟到数据，分流
            // 通过匿名内部类创建子类，所以有{}括号
        OutputTag<String> outputTag_start = new OutputTag<String>("outputTag_start", Types.STRING) {
        };
        OutputTag<String> outputTag_display = new OutputTag<String>("outputTag_display", Types.STRING) {
        };

        SingleOutputStreamOperator<String> pageDS = midWithNewFlagDS
                .process(
                        new ProcessFunction<JSONObject, String>() {

                            @Override
                            public void processElement(JSONObject jsonObj, Context ctx, Collector<String> out) throws Exception {

                                JSONObject startJsonObj = jsonObj.getJSONObject("start");
                                String dataStr = jsonObj.toString();

                                if (startJsonObj != null && startJsonObj.size() > 0) {

                                    ctx.output(outputTag_start, dataStr);
                                } else {

                                    //todo 如果不是启动日志，那么就都是页面日志
                                    out.collect(dataStr);

                                    JSONArray displays = jsonObj.getJSONArray("displays");

                                    if (displays != null && displays.size() > 0) {

                                        String page_id = jsonObj.getJSONObject("page").getString("page_id");

                                        for (int i = 0; i < displays.size(); i++) {
                                            JSONObject displayJsonObj = displays.getJSONObject(i);
                                            displayJsonObj.put("page_id", page_id);
                                            ctx.output(outputTag_display, displayJsonObj.toString());
                                        }
                                    }
                                }

                            }
                        }
                );

        //todo 获取测输出流
        DataStream<String> startDS = pageDS.getSideOutput(outputTag_start);
        DataStream<String> displayDS = pageDS.getSideOutput(outputTag_display);

        pageDS.print("page>>>");
        startDS.print("start>>>");
        displayDS.print("display>>>");


        //todo 向Kafka写入数据
        String topic_page = "dwd_page_log";
        String topic_start = "dwd_start_log";
        String topic_display = "dwd_display_log";

        FlinkKafkaProducer<String> topic_page_sink = MyKafkaUtil.getFlinkSink(topic_page);
        FlinkKafkaProducer<String> topic_start_sink = MyKafkaUtil.getFlinkSink(topic_start);
        FlinkKafkaProducer<String> topic_display_sink = MyKafkaUtil.getFlinkSink(topic_display);


        pageDS.addSink(topic_page_sink);
        startDS.addSink(topic_start_sink);
        displayDS.addSink(topic_display_sink);



        //todo 执行
        env.execute();
    }
}
