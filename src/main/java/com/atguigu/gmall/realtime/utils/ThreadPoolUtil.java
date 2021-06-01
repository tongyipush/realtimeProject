package com.atguigu.gmall.realtime.utils;

import java.util.concurrent.*;

/**
 *
 * 线程池工具类
 *
 */
public class ThreadPoolUtil {

    //todo 创建单例线程池
    public static ThreadPoolExecutor pool;

    public static ThreadPoolExecutor getInstance(){

        //todo 获取单例的线程池对象
        // corePoolSize:指定了线程池中的线程数量，它的数量决定了添加的任务是开辟新的线程去执行，
        // 还是放到workQueue任务队列中去；
        // maximumPoolSize:指定了线程池中的最大线程数量，这个参数会根据你使用的workQueue任务队列的类型，
        // 决定线程池会开辟的最大线程数量；
        // keepAliveTime:当线程池中空闲线程数量超过corePoolSize时，多余的线程会在多长时间内被销毁；
        // unit:keepAliveTime的单位
        // workQueue:任务队列，被添加到线程池中，但尚未被执行的任务

        if (pool == null){

            //todo 为防止发生线程安全问题
            // 防止a,b线程同时创建线程，所以加锁
            synchronized ( ThreadPoolUtil.class ){
                //todo 避免创建多个线程池
                if (pool == null){

                    //todo 开辟线程池
                    // 设置4个线程，如果是高峰期的话，他会自动创建，最大线程数量是20个
                    pool = new ThreadPoolExecutor(4,20,300,
                            TimeUnit.SECONDS,new LinkedBlockingQueue<>()
                    );
                }
            }
        }
        return pool;
    }
}
