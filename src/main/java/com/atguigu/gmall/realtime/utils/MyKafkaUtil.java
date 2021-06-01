package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * Author: Felix
 * Date: 2021/3/22
 * Desc: 操作Kafka的工具类
 */


public class MyKafkaUtil {

    private static String kafkaServer = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic,String groupId){
        String kafkaServer = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServer);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        return  new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
    }

    public static FlinkKafkaProducer<String> getFlinkSink(String topic){

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServer);

        //todo 把json对象转换为字符串，因为向Kafka写入数据字符串传输更加方便
        // SimpleStringSchema字符串序列化方式
        return new FlinkKafkaProducer<String>(topic,new SimpleStringSchema(),props);
    }

    //todo 因为流中数据类型不确定，所以需要自己实现序列化
    // 获取Kafkasink 一个流中的数据发送到不同的主题中
    public static <T>FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema){
        String DEFAULT_TOPIC = "default_topic";
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServer);
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"");

        FlinkKafkaProducer<T> jsonObjectFlinkKafkaProducer = new FlinkKafkaProducer<T>(DEFAULT_TOPIC,
                kafkaSerializationSchema, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        return jsonObjectFlinkKafkaProducer;
    }

    //拼接Kafka相关属性到DDL
    public static String getKafkaDDL(String topic,String groupId){
        String kafkaServer = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
        String ddl="'connector' = 'kafka', " +
                " 'topic' = '"+topic+"',"   +
                " 'properties.bootstrap.servers' = '"+ kafkaServer +"', " +
                " 'properties.group.id' = '"+groupId+ "', " +
                "  'format' = 'json', " +
                "  'scan.startup.mode' = 'latest-offset'  ";
        return  ddl;
    }
}
