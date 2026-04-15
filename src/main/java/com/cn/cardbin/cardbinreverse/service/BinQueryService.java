package com.cn.cardbin.cardbinreverse.service;

import com.cn.cardbin.cardbinreverse.dto.BinInfo;
import com.cn.cardbin.cardbinreverse.dto.QueryRequest;
import com.cn.cardbin.cardbinreverse.dto.QueryResponse;
import com.cn.cardbin.cardbinreverse.engine.BinStorageEngine;
import com.cn.cardbin.cardbinreverse.entity.BinRecord;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class BinQueryService {

    private final BinStorageEngine storageEngine;
    private final BinDataLoader dataLoader;
    private final ThreadPoolExecutor threadPoolExecutor;

    // 统计信息
    private final AtomicLong totalRequests = new AtomicLong(0);
    private final AtomicLong successfulRequests = new AtomicLong(0);
    private final Map<String, AtomicLong> methodCounts = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicLong> responseTimeBuckets = new ConcurrentHashMap<>();
    private final int[] binLengths;
    @Value("${app.response.timeout:100}")
    private long responseTimeout;



    /**
     * 查询BIN信息
     */
    public QueryResponse query(QueryRequest request) {
        totalRequests.incrementAndGet();
        StopWatch watch = new StopWatch();
        watch.start();

        try {
            String cardNumber = request.getCardNumber();
            //最小卡长度
            if (cardNumber == null || cardNumber.length() < binLengths[0]) {
                return buildErrorResponse(request, "Invalid card number");
            }
            // 查询BIN信息
            BinRecord record = storageEngine.query(cardNumber, binLengths);
            log.info("query cardNumber:[{}]", cardNumber);
            if (record == null) {
                return buildErrorResponse(request, "BIN not found");
            }

            successfulRequests.incrementAndGet();

            watch.stop();
            recordResponseTime(watch.getTotalTimeMillis());

            return QueryResponse.builder()
                    .requestId(request.getRequestId())
                    .cardNumber(cardNumber)
                    .binInfo(convertToBinInfo(record))
                    .cacheHit(isCacheHit())
                    .responseTimeNs(watch.getTotalTimeNanos())
                    .build();

        } catch (Exception e) {
            log.error("Query failed for request: {}", request, e);
            return buildErrorResponse(request, "Internal server error");
        }
    }
    /**
     * 异步查询
     */
    public CompletableFuture<QueryResponse> queryAsync(QueryRequest request) {
         return CompletableFuture.supplyAsync(() -> query(request), threadPoolExecutor)
                .orTimeout(responseTimeout, TimeUnit.MILLISECONDS)
                .exceptionally(ex -> {
                    log.error("Async query timeout for request: {}", request, ex);
                    return buildErrorResponse(request, "Query timeout");
                });
    }
    /**
     * 批量查询
     */
    public List<QueryResponse> batchQuery(List<QueryRequest> requests) {
        if (requests.size() > 1000) {
            throw new IllegalArgumentException("Batch size cannot exceed 1000");
        }

        // 提取卡号
        List<String> cardNumbers = requests.stream()
                .map(QueryRequest::getCardNumber)
                .collect(Collectors.toList());

        // 批量查询
        //Map<String, BinRecord> results = storageEngine.batchQuery(cardNumbers);
        Map<String, BinRecord> results = storageEngine.batchQuery(cardNumbers,binLengths);

        // 构建响应
        return requests.stream()
                .map(request -> {
                    BinRecord record = results.get(request.getCardNumber());

                    return QueryResponse.builder()
                            .requestId(request.getRequestId())
                            .cardNumber(request.getCardNumber())
                            .binInfo(record != null ? convertToBinInfo(record) : null)
                            .cacheHit(false) // 批量查询不记录缓存命中
                            .responseTimeNs(0L)
                            .build();
                })
                .collect(Collectors.toList());
    }

    /**
     * 预热热点数据
     */
    public void preheatHotData() {
        // 预加载热门卡BIN
        String[] hotBin = {
                "621226", "622848", "621700", "621669",
                "621799", "621788", "621738", "621483"
        };
        List<String> hotBins = Arrays.asList(hotBin);
        hotBins.forEach(bin -> {
            // 模拟卡号查询，触发缓存
            String mockCard = bin + "0000000000";
            storageEngine.query(mockCard,6);
        });

        log.info("Hot data preheating completed");
    }

    /**
     * 获取服务统计
     */
    public Map<String, Object> getServiceStats() {
        Map<String, Object> stats = storageEngine.getStats();
        stats.put("totalRequests", totalRequests.get());
        stats.put("successfulRequests", successfulRequests.get());
        stats.put("successRate", calculateSuccessRate());
        stats.put("methodCounts", methodCounts);
        stats.put("responseTimeDistribution", getResponseTimeDistribution());

        return stats;
    }

    /**
     * 定时清理统计
     */
    @Scheduled(cron = "0 0 0 * * ?") // 每天凌晨执行
    public void resetDailyStats() {
        totalRequests.set(0);
        successfulRequests.set(0);
        methodCounts.clear();
        responseTimeBuckets.clear();
        log.info("Daily statistics reset");
    }

    /**
     * 刷新BIN数据
     */
    @Scheduled(cron = "0 0 2 * * ?") // 每天凌晨2点执行
    public void refreshBinData() {
        log.info("Starting BIN data refresh...");
        dataLoader.loadData();
        log.info("BIN data refresh completed");

        log.info("Starting LOCATION data refresh...");
        dataLoader.loadLocation();
        log.info("LOCATION data refresh completed");
    }

    // 私有方法
    private QueryResponse buildErrorResponse(QueryRequest request, String error) {
        return QueryResponse.builder()
                .requestId(request.getRequestId())
                .cardNumber(request.getCardNumber())
                .binInfo(null)
                .cacheHit(false)
                .responseTimeNs(0L)
                .build();
    }

    private BinInfo convertToBinInfo(BinRecord record) {
        return BinInfo.builder()
                .prefix(record.getPrefix())
                .issuer(record.getIssuer())
                .cardType(record.getCardType())
                .country(record.getCountry())
                .province(record.getProvince())
                .city(record.getCity())
                .organization(record.getOrganization())
                .build();
    }

    private void recordResponseTime(long timeMs) {
        int bucket = (int) (timeMs / 10) * 10; // 10ms为桶
        responseTimeBuckets
                .computeIfAbsent(bucket, k -> new AtomicLong(0))
                .incrementAndGet();
    }

    private boolean isCacheHit() {
        // 这里可以记录缓存命中状态
        return true;
    }

    private double calculateSuccessRate() {
        long total = totalRequests.get();
        long success = successfulRequests.get();
        return total > 0 ? (double) success / total * 100 : 0;
    }

    private Map<String, Long> getResponseTimeDistribution() {
        return responseTimeBuckets.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> e.getKey() + "ms",
                        e -> e.getValue().get()
                ));
    }
}

