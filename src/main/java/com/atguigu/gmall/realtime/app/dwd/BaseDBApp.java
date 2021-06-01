package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimSink;
import com.atguigu.gmall.realtime.app.func.TableProcessFunction;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 *启动进行
 *
 * DB业务数据处理流程
 *  1.运行模拟数据生成的jar,将生成数据发送到MySQL
 *  2.MySQL开启binlog，监控某个数据库gmall2020的变化
 *      sudo vim /etc/my.cnf修改MySQL配置文件
 *      server-id= 1
 *      log-bin=mysql-bin
 *      binlog_format=row
 *      binlog-do-db=gmall2020
 *      binlog-do-db=gmall2021
 *      二进制日志文件Binarylog存成二进制的原因：加密
 *      {binlog录了所有的DDL和DML(除了数据查询语句)语句，以事件形式记录，
 *      还包含语句所执行的消耗的时间，MySQL的二进制日志是事务安全型的。}
 *  3.使用Maxwell监控MySQL数据变化，并将其发送到ods层：Kafka主题ods_base_db_m
 *      (1)分配Mysql账号可以监控操作该数据库
 *          GRANT ALL   ON maxwell.* TO 'maxwell'@'%' IDENTIFIED BY '123456';
 *          分配这个账号，可以监控其他数据库的权限
 *          GRANT  SELECT ,REPLICATION SLAVE , REPLICATION CLIENT  ON *.* TO maxwell@'%';
 *      (2)修改Maxwell配置文件config.properties
 *      producer=kafka
 *      kafka.bootstrap.servers=hadoop202:9092,hadoop203:9092,hadoop204:9092
 *      #需要添加
 *      kafka_topic=ods_base_db_m
 *      # mysql login info
 *      host=hadoop202
 *      user=maxwell
 *      password=123456
 *      #需要添加 后续初始化会用
 *      client_id=maxwell_1
 *  4.使用BaseDBApp从Kafka主题ods_base_db_m中读取数据，根据配置数据库gmall_realtime：table_process信息
 *  判断将数据流发送到DIM层：hbase[维度表]和DWD层：Kafka[事实表]中不同事实表主题中；例：dwd_order_info主题中
 *      (1)配置数据库gmall_realtime：table_process维护的字段类型为：其全部为事实表的字段
 *          将字段封装为TableProcess类
 *          source_table  来源表
 *          operate_type  操作类型
 *          sink_type     输出类型
 *          sink_table    输出表(主题)
 *          sink_columns  输出字段
 *          sink_pk       主键字段
 *          sink_extend   建表扩展
 *      (2)使用MySQLUtil工具类读取gmall_realtime数据库，将每一行数据封装为Process类，返回list<TableProcess>
 *      (3)对从MySQL数据库中取出的List集合进行遍历，缓存到HashMap内存中，tableProcessMap.put(key,tableProcess)
 *      (4)新建Hashset集合，boolean notExit = set.add(sourceTable)，根据返回值判断是否hbase中是否存在这张表，没有就新建
 *          新建表：根据table process中取出的表名，字段，主键等，新建建表字符串BuilderString，建立phoneix连接，创建表
 *      (5)对从ods_base_db_m中获取的数据datajsonObj.getJSONObject("data")，获取表名table，操作类型type，组成key=>table:type,
 *      在hashmap中查取结果, TableProcess tableProcess = tableProcessMap.get(key)，根据table Process.getSinkType()判断
 *      分流：侧输出流hbase,主流Kafka的dwd层中不同事实表主题：dwd_order_info
 *          类型修复：jsonObj.put("type",type);
 *          对数据jsonObj数据流添加目标主题或者表：jsonObj.put("sink_table",tableProcess.getSinkTable())
 *          数据过滤：根据tableProcess.getSinkColumns()中的列字段过滤jsonObj中的数据
 *                    使用迭代器的remove功能：dataJsonObj<JSONObject>.entrySet().iterator()=>!fieldList.contains(entry.getKey()
 *                                          => iterator.remove()
 *                                          数组转换为集合，因为集合包含contain方法：Arrays.asList(fieldArr)
 *      (6)建立phenoix连接，向hbase[海量存储，根据rowkey查询效率高]数据库中写入dim层数据
 *             在jsonObj中取出目标表：tableName = jsonObj.getString("sink_table")
 *      (7)向Kafka中dwd维度表中主题[dwd_order_info]写入数据
 *              在jsonObj中取出目标主题：topicName = jsonObj.getString("sink_table")
 *          新建Kafka生产者：new FlinkKafkaProducer<String>
 *              string类型采用：new SimpleStringSchema()序列化方式
 *              JSONObj采用：kafkaSerializationSchema字节序列化方式
 *      (8)
 *
 */

public class BaseDBApp {
    public static void main(String[] args) throws Exception {

        //todo 1.创建基本环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //todo 检查点相关的配置
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9820/gmall0921/flink/checkpoint"));
//        env.setRestartStrategy(RestartStrategies.noRestart());

        //todo 从kafka主题中读取数据
        String topic = "ods_base_db_m";
        String groupId = "basedbapp_group";

        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String jsonStr) throws Exception {
                        return JSON.parseObject(jsonStr);
                    }
                }
        );

        //过滤
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        boolean flag = jsonObject.getString("table") != null
                                && jsonObject.getString("table").length() > 0
                                && jsonObject.getString("data") != null
                                && jsonObject.getString("data").length() > 3;

                        return flag;
                    }
                }
        );

//        filterDS.print("DB3>>>");

        //todo 对数据进行分流操作，维度数据放到测输出流，事实数据放到主流
        // 使用匿名内部类，所以需要使用{}
        OutputTag<JSONObject> dimTag = new OutputTag<JSONObject>("dim_Tag"){};
        SingleOutputStreamOperator<JSONObject> realDS = filterDS.process(new TableProcessFunction(dimTag));

        //todo  获取数据流
        DataStream<JSONObject> dimDS = realDS.getSideOutput(dimTag);

        dimDS.print("维度表，hbase侧输出流");

        realDS.print("事实表，kafka主流");

        //todo 将维度数据通过phoenix写到hbase中
        // 通过phenoix连接
        // 拼接sql
        // 将结果通过phonenix写入hbase
        dimDS.addSink(
            new DimSink()
        );
        //todo 将事实表写到Kafka的不同的dwd主题中
        // dwd_order_info


        realDS.addSink(MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObj, @Nullable Long timestamp) {

                //todo 保存到Kafka的一个主题
                String topic = jsonObj.getString("sink_table");
                //todo 获取数据
                JSONObject dataJsonObj = jsonObj.getJSONObject("data");
                //todo 传key取hash，null是轮询，不传值粘性分区
                return new ProducerRecord<>(topic,dataJsonObj.toString().getBytes());
            }
        }));


        //todo 执行
        env.execute();
    }
}
