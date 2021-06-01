package com.atguigu.gmall.realtime.common;
/**
 * Author: Felix
 * Date: 2021/3/23
 * Desc: 项目配置类
 */
public class GmallConfig {
    public static final String HBASE_SCHEMA="gmall_flink_hbase";
    public static final String PHOENIX_SERVER="jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

    public static final String CLICKHOUSE_URL="jdbc:clickhouse://hadoop102:8123/default";
}
