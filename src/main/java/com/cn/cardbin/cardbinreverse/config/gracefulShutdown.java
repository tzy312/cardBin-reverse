package com.cn.cardbin.cardbinreverse.config;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
//import javax.annotation.PreDestroy;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class gracefulShutdown {
    private final ThreadPoolExecutor threadPoolExecutor;
    @PreDestroy
    public void destroy() throws Exception {
        // 优雅关闭线程池
        log.warn("关闭线程池-不再接受新任务 ...");
        threadPoolExecutor.shutdown(); // 不再接受新任务
        try {
            // 等待现有任务完成，最多等待5秒
            if (!threadPoolExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                // 如果超时，尝试取消所有正在执行的任务
                threadPoolExecutor.shutdownNow();
                // 再次等待一段时间，如果还有任务没有取消完成
                if (!threadPoolExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.warn("线程池未能完全关闭");
                }
            }
        } catch (InterruptedException e) {
            // 如果当前线程被中断，重新尝试取消所有任务
            threadPoolExecutor.shutdownNow();
            // 保持中断状态
            Thread.currentThread().interrupt();
        }
        log.warn("关闭线程池 successful ...");
    }
}
