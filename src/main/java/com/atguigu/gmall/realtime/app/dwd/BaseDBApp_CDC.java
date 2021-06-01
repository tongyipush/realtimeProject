package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.gmall.realtime.app.func.DimSink;
import com.atguigu.gmall.realtime.app.func.MyDeserializationSchemaFunction_CDC;
import com.atguigu.gmall.realtime.app.func.TableProcessFunction;
import com.atguigu.gmall.realtime.app.func.TableProcessFunction_CDC;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * CDC Change Data Capture变化数据捕获
 * canal,maxwell都是CDC的变化工具
 * 传统数据实现：下单=>将业务写入数据库，把数据写道esticsearch,写入缓存，这样一致性得不到保证
 * 优化=>下单=>将业务写入数据库=>（通过canal,maxwell采集）数据写到esticsearch,写入缓存
 * flinkcdc和maxwell,canal的作用是一样的，优点是把数据采集做了更加深层次的风封装，底层是Debezium
 *  1.未封装之前
 *      Debezium把业务数据库库数据采集过来发往Kafka，交给计算引擎计算最后把数据保存到hadoop或者elasticsearch
 *  2.flinkCDCfe封装之后
 *      CDC直接采集业务数据发往flink
 * 在官网是看不到，是社区的
 * 的底层是
 *
 */
public class BaseDBApp_CDC {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //Flink流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

       /*
       //设置CK相关参数
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/flink/checkpoint"));
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        */

        //TODO 2.接收Kafka数据，过滤空值数据
        //定义消费者组以及指定消费主题
        String topic = "ods_base_db_m";
        String groupId = "ods_base_group";

        //从Kafka主题中读取数据
        FlinkKafkaConsumer<String> kafkaSource  = MyKafkaUtil.getKafkaSource(topic,groupId);
        DataStream<String> jsonDstream   = env.addSource(kafkaSource);
        //jsonDstream.print("data json:::::::");

        //对数据进行结构的转换   String->JSONObject
        DataStream<JSONObject>  jsonStream   = jsonDstream.map(jsonStr -> JSON.parseObject(jsonStr));
        //DataStream<JSONObject>  jsonStream   = jsonDstream.map(JSON::parseObject);

        //过滤为空或者 长度不足的数据
        SingleOutputStreamOperator<JSONObject> filteredDstream = jsonStream.filter(
                jsonObject -> {
                    boolean flag = jsonObject.getString("table") != null
                            && jsonObject.getJSONObject("data") != null
                            && jsonObject.getString("data").length() > 3;
                    return flag;
                }) ;
        //filteredDstream.print("json::::::::");


        //TODO 3.0-1 使用FlinkCDC读取配置表形成广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall000_realtime")
                .tableList("gmall000_realtime.table_process")
                .deserializer(new MyDeserializationSchemaFunction_CDC())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> mysqlDS = env.addSource(sourceFunction);

        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("table-process", String.class, TableProcess.class);
        //todo 广播状态，只能默认Map状态
        BroadcastStream<String> broadcastStream = mysqlDS.broadcast(mapStateDescriptor);

        //TODO 3.0-2 连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = filteredDstream.connect(broadcastStream);

        //TODO 3. 对广播流进行 分流  将维度数据放到侧输出流   事实数据放到主流
        OutputTag<JSONObject> dimTag = new OutputTag<JSONObject>("dimTag"){};

        SingleOutputStreamOperator<JSONObject> realDS = connectedStream.process(new TableProcessFunction_CDC(dimTag,mapStateDescriptor));

        //获取维度
        DataStream<JSONObject> dimDS = realDS.getSideOutput(dimTag);

        dimDS.print(">>>");
        realDS.print("###");

        // 将维度数据写到Hbase
        dimDS.addSink(new DimSink());

        //将数据写到kafka中
        realDS.addSink(
                MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObj, @Nullable Long timestamp) {
                        String topicName = jsonObj.getString("sink_table");

                        return new ProducerRecord<byte[], byte[]>(topicName,jsonObj.getJSONObject("data").toJSONString().getBytes());
                    }
                })
        );
        env.execute();
    }

}
