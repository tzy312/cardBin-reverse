package com.cn.cardbin.cardbinreverse.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Configuration
public class GlobleThreadPool {
    @Value("${server.threadPool.maxSise:16}")
    private int maxSize;
    @Value("${server.threadPool.minSise:8}")
    private int minSize;

    @Bean("threadPoolExecutor")
    public ThreadPoolExecutor threadPoolExecutor(){
        return new ThreadPoolExecutor(minSize,
                maxSize,
                0l, 
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(100),
                tzyThreadFactory("tzy-pool-thread"),
                new ThreadPoolExecutor.AbortPolicy());
    }
    public ThreadFactory tzyThreadFactory(String poolName){
        return new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r,poolName );
                t.setDaemon(false);
                t.setPriority(Thread.MAX_PRIORITY);
                return t;
            }
        };
    }
}
