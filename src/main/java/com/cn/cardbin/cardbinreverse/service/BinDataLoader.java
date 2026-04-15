package com.cn.cardbin.cardbinreverse.service;

import com.cn.cardbin.cardbinreverse.engine.BinStorageEngine;
import com.cn.cardbin.cardbinreverse.entity.BinLocation;
import com.cn.cardbin.cardbinreverse.entity.BinRecord;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class BinDataLoader {

    private final BinStorageEngine storageEngine;

    @Value("${app.bin.data.file:bin_data.csv}")
    private String dataFile;

    @Value("${app.bin.data.fileLocation:bin_location.csv}")
    private String locationFile;

    @Value("${app.bin.data.preload:true}")
    private boolean preload;

    @PostConstruct
    public void init() {
        if (preload) {
            loadData();
            loadLocation();
        }
    }

    /**
     * 加载BIN数据
     */
    @Async
    public CompletableFuture<Void> loadData() {
        log.info("Start loading BIN data from: {}", dataFile);
        CSVReader reader = null;
        try {
            ClassPathResource resource = new ClassPathResource(dataFile);
            reader = new CSVReaderBuilder(new InputStreamReader(resource.getInputStream(), "UTF-8"))
                    .withSkipLines(1) // 跳过标题行
                    .build();

            List<BinRecord> records = new ArrayList<>();
            String[] nextLine;
            int count = 0;

            while ((nextLine = reader.readNext()) != null) {
                if (nextLine.length >= 8) {
                    try {
                        BinRecord record = BinRecord.fromCsv(nextLine);
                        records.add(record);
                        count++;

                        // 分批处理
                        if (records.size() >= 10000) {
                            storageEngine.loadData(records);
                            records.clear();
                        }
                    } catch (Exception e) {
                        log.warn("Failed to parse line: {}", (Object) nextLine, e);
                    }
                }
            }

            // 加载剩余数据
            if (!records.isEmpty()) {
                storageEngine.loadData(records);
            }

            log.info("Loaded {} BIN records from {}", count, dataFile);

        } catch (Exception e) {
            log.error("Failed to load BIN data", e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        return CompletableFuture.completedFuture(null);
    }

    /**
     * 加载LOCATION数据
     */
    @Async
    public CompletableFuture<Void> loadLocation() {
        log.info("Start loading LOCATION data from: {}", locationFile);
        CSVReader reader = null;
        try {
            ClassPathResource resource = new ClassPathResource(locationFile);
            reader = new CSVReaderBuilder(new InputStreamReader(resource.getInputStream(), "UTF-8"))
                    .withSkipLines(1) // 跳过标题行
                    .build();

            List<BinLocation> records = new ArrayList<>();
            String[] nextLine;
            int count = 0;

            while ((nextLine = reader.readNext()) != null) {
                if (nextLine.length >= 4) {
                    try {
                        BinLocation record = BinLocation.fromCsv(nextLine);
                        records.add(record);
                        count++;

                        // 分批处理
                        if (records.size() >= 10000) {
                            storageEngine.loadLocation(records);
                            records.clear();
                        }
                    } catch (Exception e) {
                        log.warn("Failed to parse line: {}", (Object) nextLine, e);
                    }
                }
            }
            // 加载剩余数据
            if (!records.isEmpty()) {
                storageEngine.loadLocation(records);
            }

            log.info("Loaded {} LOCATION records from {}", count, locationFile);

        } catch (Exception e) {
            log.error("Failed to load LOCATION data", e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return CompletableFuture.completedFuture(null);
    }
}

