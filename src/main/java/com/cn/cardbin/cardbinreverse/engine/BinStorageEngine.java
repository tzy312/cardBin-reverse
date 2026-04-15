package com.cn.cardbin.cardbinreverse.engine;

import com.cn.cardbin.cardbinreverse.dto.BinInfo;
import com.cn.cardbin.cardbinreverse.entity.BinLocation;
import com.cn.cardbin.cardbinreverse.entity.BinRecord;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.coyote.http11.Http11Nio2Protocol;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
@Component
public class BinStorageEngine {

    // 多级索引结构
    private final Map<String, BinRecord> primaryIndex = new ConcurrentHashMap<>(100000);
    private final Map<String, BinLocation> primaryLocationIndex = new ConcurrentHashMap<>(100000);

    private final ConcurrentSkipListMap<String, BinRecord> rangeIndex = new ConcurrentSkipListMap<>();

    // 布隆过滤器用于快速判断是否存在
    private BloomFilter<String> bloomFilter;

    // 高性能本地缓存
    private final Cache<String, BinRecord> localCache = Caffeine.newBuilder()
            .maximumSize(100000)
            .expireAfterWrite(1, java.util.concurrent.TimeUnit.HOURS)
            .recordStats()
            .build();
    // 高性能本地缓存
    private final Cache<String, BinLocation> locationCache = Caffeine.newBuilder()
            .maximumSize(100000)
            .expireAfterWrite(1, java.util.concurrent.TimeUnit.HOURS)
            .recordStats()
            .build();
    // 统计信息
    private final AtomicLong queryCount = new AtomicLong(0);
    private final AtomicLong cacheHitCount = new AtomicLong(0);
    private final AtomicLong bloomFilterHitCount = new AtomicLong(0);

    // 读写锁
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final ReentrantReadWriteLock lockLocation = new ReentrantReadWriteLock();

    public BinStorageEngine() {
        initialize();
    }

    private void initialize() {
        // 初始化布隆过滤器
        bloomFilter = BloomFilter.create(
                Funnels.stringFunnel(StandardCharsets.UTF_8),
                1000000,  // 预计元素数量
                0.01      // 误判率
        );

        // 启动统计线程
        new Thread(this::collectStats, "BinStorage-Stats-Collector").start();
    }

    /**
     * 批量加载BIN数据
     */
    public void loadData(List<BinRecord> records) {
        StopWatch watch = new StopWatch();
        watch.start();

        lock.writeLock().lock();
        try {
            for (BinRecord record : records) {
                String prefix = record.getPrefix();
                primaryIndex.put(prefix, record);
                rangeIndex.put(prefix, record);
                bloomFilter.put(prefix);
            }

            // 预热缓存
            preheatCache(records);

            watch.stop();
            log.info("Loaded {} BIN records in {} ms", records.size(), watch.getTotalTimeMillis());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 批量加载LOCATION数据
     */
    public void loadLocation(List<BinLocation> records) {
        StopWatch watch = new StopWatch();
        watch.start();

        lockLocation.writeLock().lock();
        try {
            for (BinLocation record : records) {
                String prefix = record.getIssuer().concat("-").concat(record.getCode());
                primaryLocationIndex.put(prefix, record);
                bloomFilter.put(prefix);
            }
            // 预热缓存
            preheatLocationCache(records);
            watch.stop();
            log.info("Loaded {} LOCATION records in {} ms", records.size(), watch.getTotalTimeMillis());
        } finally {
            lockLocation.writeLock().unlock();
        }
    }
    /**
     * 预热缓存
     */
    private void preheatCache(List<BinRecord> records) {
        int preheatSize = Math.min(10000, records.size());
        Random random = new Random();

        for (int i = 0; i < preheatSize; i++) {
            BinRecord record = records.get(random.nextInt(records.size()));
            localCache.put(record.getPrefix(), record);
        }

        log.info("Cache preheated with {} records", preheatSize);
    }
    /**
     * 预热缓存
     */
    private void preheatLocationCache(List<BinLocation> records) {
        int preheatSize = Math.min(10000, records.size());
        Random random = new Random();

        for (int i = 0; i < preheatSize; i++) {
            BinLocation record = records.get(random.nextInt(records.size()));
            locationCache.put(record.getIssuer().concat("-").concat(record.getCode()), record);
        }

        log.info("Cache preheated with {} records", preheatSize);
    }
    /**
     * 查询BIN信息 - 高性能实现
     */
    public BinRecord query(String cardNumber,int binLength) {
        queryCount.incrementAndGet();

        if (cardNumber == null || cardNumber.length() < binLength) {
            return null;
        }

        String prefix = cardNumber.substring(0, binLength);

        // 1. 先查本地缓存
        BinRecord cached = localCache.getIfPresent(prefix);
        if (cached != null) {
            cacheHitCount.incrementAndGet();
            return cached;
        }

        // 2. 布隆过滤器快速判断是否存在
        if (!bloomFilter.mightContain(prefix)) {
            bloomFilterHitCount.incrementAndGet();
            return null;
        }

        // 3. 查询主索引
        lock.readLock().lock();
        try {
            BinRecord record = primaryIndex.get(prefix);

            if (record != null) {
                // 放入缓存
                localCache.put(prefix, record);
            }
            return record;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 批量查询 - 优化性能
     */
    public Map<String, BinRecord> batchQuery(List<String> cardNumbers) {
        Map<String, BinRecord> results = new HashMap<>(cardNumbers.size());

        // 分批处理，减少锁竞争
        int batchSize = 1000;
        for (int i = 0; i < cardNumbers.size(); i += batchSize) {
            int end = Math.min(i + batchSize, cardNumbers.size());
            List<String> batch = cardNumbers.subList(i, end);

            Map<String, BinRecord> batchResults = new HashMap<>();
            List<String> needDbQuery = new ArrayList<>();

            // 先查缓存
            for (String cardNumber : batch) {
                if (cardNumber.length() < 6) continue;

                String prefix = cardNumber.substring(0, 6);
                BinRecord cached = localCache.getIfPresent(prefix);
                if (cached != null) {
                    batchResults.put(cardNumber, cached);
                } else {
                    needDbQuery.add(cardNumber);
                }
            }

            // 批量查询数据库
            if (!needDbQuery.isEmpty()) {
                lock.readLock().lock();
                try {
                    for (String cardNumber : needDbQuery) {
                        String prefix = cardNumber.substring(0, 6);
                        BinRecord record = primaryIndex.get(prefix);
                        if (record != null) {
                            batchResults.put(cardNumber, record);
                            localCache.put(prefix, record);
                        }
                    }
                } finally {
                    lock.readLock().unlock();
                }
            }

            results.putAll(batchResults);
        }

        return results;
    }
    /**
     * 查询BIN信息 - 高性能实现
     */
    public BinRecord query(String cardNumber,int[] binLengths) {
        queryCount.incrementAndGet();
        if (cardNumber == null || cardNumber.length() < binLengths[0]) {
            return null;
        }
        String prefix = null;
        // 1. 先查本地缓存
        BinRecord cached = null;
        boolean bloomFilterCheck = false;
        for (int k = binLengths.length; k > 0 ; k--) {
            if(cardNumber.length() > binLengths[k-1]){
                prefix = cardNumber.substring(0, binLengths[k-1]);
                if(bloomFilter.mightContain(prefix)){
                    bloomFilterCheck = true;
                }
                cached = localCache.getIfPresent(prefix);
            }
            if (null != cached) {
                break;  //获取数据并跳出循环，减少查询次数
            }
        }

        if (cached != null) {
            cacheHitCount.incrementAndGet();
            cached = getLocationFromBin(cardNumber, cached);
            return cached;
        }
        // 2. 布隆过滤器快速判断是否存在
        if (!bloomFilterCheck) {
            bloomFilterHitCount.incrementAndGet();
            return null;
        }

        // 3. 查询主索引
        lock.readLock().lock();
        try {
            BinRecord record = null;
            for (int k = binLengths.length; k > 0 ; k--) {
                if(cardNumber.length() > binLengths[k-1]){
                    prefix = cardNumber.substring(0, binLengths[k-1]);
                    record = primaryIndex.get(prefix);
                }
                if (null != record) {
                    break;  //获取数据并跳出循环，减少查询次数
                }
            }
            if (record != null) {
                // 放入缓存
                localCache.put(prefix, record);
                record = getLocationFromBin(cardNumber, record);
            }
            return record;
        } finally {
            lock.readLock().unlock();
        }
    }
    /**
     * 进一步通过地区代码查询卡归属地
     */
    public BinRecord getLocationFromBin(String cardNumberNo,BinRecord binRecord){
        if (null != binRecord ){
            if (StringUtils.isNotEmpty(binRecord.getLocationCode())
                    && StringUtils.isNotEmpty(binRecord.getLocationCodeLength())){

                int start = Integer.valueOf(binRecord.getLocationCode()) - 1;
                int end = start + Integer.valueOf(binRecord.getLocationCodeLength());
                String code = cardNumberNo.substring(start,end);
                String prefix = binRecord.getIssuer().concat("-").concat(code);
                // 布隆过滤器快速判断是否存在
                if (!bloomFilter.mightContain(prefix)) {
                    bloomFilterHitCount.incrementAndGet();
                    binRecord.setProvince("");
                    binRecord.setOrganization("");
                    return binRecord;
                }
                BinLocation binLocation = locationCache.getIfPresent(prefix);
                if (null != binLocation){
                    binRecord.setProvince(binLocation.getProvince());
                    binRecord.setOrganization(binLocation.getOrganization());
                    return binRecord;
                }
                lockLocation.readLock().lock();
                try {
                    BinLocation location = primaryLocationIndex.get(prefix);
                    if (location != null) {
                        binRecord.setProvince(location.getProvince());
                        binRecord.setOrganization(location.getOrganization());
                        locationCache.put(prefix,location);
                    }
                    return binRecord;
                } finally {
                    lockLocation.readLock().unlock();
                }
            }
        }
        return binRecord;
    }
    /**
     * 批量查询 - 优化
     */
    public Map<String, BinRecord> batchQuery(List<String> cardNumbers,int[] binLengths) {
        Map<String, BinRecord> results = new HashMap<>(cardNumbers.size());

        // 分批处理，减少锁竞争
        int batchSize = 200;
        for (int i = 0; i < cardNumbers.size(); i += batchSize) {
            int end = Math.min(i + batchSize, cardNumbers.size());
            List<String> batch = cardNumbers.subList(i, end);

            Map<String, BinRecord> batchResults = new HashMap<>();
            List<String> needDbQuery = new ArrayList<>();

            // 先查缓存
            for (String cardNumber : batch) {
                if (cardNumber.length() < binLengths[0]) continue;

                BinRecord cached = null;
                for (int k = binLengths.length; k > 0 ; k--) {
                    if(cardNumber.length() > binLengths[k-1]){
                        cached = localCache.getIfPresent(cardNumber.substring(0, binLengths[k-1]));
                    }
                    if (null != cached) {
                        break;  //获取数据并跳出循环，减少查询次数
                    }
                }
                /*String prefix = cardNumber.substring(0, 6);
                BinRecord cached = localCache.getIfPresent(prefix);*/
                if (cached != null) {
                    cached = getLocationFromBin(cardNumber, cached);
                    batchResults.put(cardNumber, cached);
                } else {
                    needDbQuery.add(cardNumber);
                }
            }

            // 批量查询数据库
            if (!needDbQuery.isEmpty()) {
                lock.readLock().lock();
                try {
                    for (String cardNumber : needDbQuery) {
                        BinRecord record = null;
                        String prefix = null;
                        for (int k = binLengths.length; k > 0 ; k--) {
                            if(cardNumber.length() > binLengths[k-1]){
                                prefix = cardNumber.substring(0, binLengths[k-1]);
                                record = primaryIndex.get(prefix);
                            }
                            if (null != record) {
                                break;  //获取数据并跳出循环，减少查询次数
                            }
                        }
                       /* String prefix = cardNumber.substring(0, 6);
                        BinRecord record = primaryIndex.get(prefix);*/
                        if (record != null) {
                            record = getLocationFromBin(cardNumber, record);
                            batchResults.put(cardNumber, record);
                            localCache.put(prefix, record);
                        }
                    }
                } finally {
                    lock.readLock().unlock();
                }
            }

            results.putAll(batchResults);
        }

        return results;
    }
    /**
     * 范围查询
     */
    public List<BinRecord> rangeQuery(String from, String to) {
        lock.readLock().lock();
        try {
            SortedMap<String, BinRecord> subMap = rangeIndex.subMap(from, to);
            return new ArrayList<>(subMap.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取统计信息
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalRecords", primaryIndex.size());
        stats.put("totalQueries", queryCount.get());
        stats.put("cacheHits", cacheHitCount.get());
        stats.put("bloomFilterHits", bloomFilterHitCount.get());
        stats.put("cacheSize", localCache.estimatedSize());
        stats.put("cacheStats", localCache.stats());

        double hitRate = queryCount.get() > 0 ?
                (double) cacheHitCount.get() / queryCount.get() * 100 : 0;
        stats.put("cacheHitRate", String.format("%.2f%%", hitRate));

        return stats;
    }

    /**
     * 收集统计信息
     */
    private void collectStats() {
        while (true) {
            try {
                Thread.sleep(60_1000); // 每分钟收集一次
                Map<String, Object> stats = getStats();
                log.info("Storage Engine Stats: {}", stats);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
