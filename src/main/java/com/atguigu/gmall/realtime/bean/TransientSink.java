package com.atguigu.gmall.realtime.bean;


import org.apache.yetus.audience.InterfaceStability;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 *
 * 标记不需要保存到clickHouse的属性注解
 */

//todo 标识加在字段上
@Target(FIELD)
//todo 标识注解的作用范围，
// 只在源码范围有效、在类的范围有效、在运行环境有效
@Retention(RUNTIME)
public @interface TransientSink {
}
