package com.cn.cardbin.cardbinreverse.controller;

import com.cn.cardbin.cardbinreverse.dto.ApiResponse;
import com.cn.cardbin.cardbinreverse.dto.QueryRequest;
import com.cn.cardbin.cardbinreverse.dto.QueryResponse;
import com.cn.cardbin.cardbinreverse.service.BinQueryService;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Bucket4j;
import io.github.bucket4j.Refill;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.async.DeferredResult;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
@RestController
@RequestMapping("/api/v1/bin")
@RequiredArgsConstructor
public class BinQueryController {

    private final BinQueryService binQueryService;

    // 限流桶
    private final Map<String, Bucket> ipBuckets = new ConcurrentHashMap<>();

    // 全局限流
    private final Bucket globalBucket = Bucket4j.builder()
            .addLimit(Bandwidth.classic(10000, Refill.greedy(10000, Duration.ofSeconds(1))))
            .build();

    /**
     * 查询卡BIN
     */
    @PostMapping("/query")
    public DeferredResult<ResponseEntity<ApiResponse<QueryResponse>>> query(
            @RequestBody QueryRequest request,
            @RequestHeader(value = "X-Request-ID", required = false) String requestId,
            @RequestHeader(value = "X-Client-IP", required = false) String clientIp) {
        long startTime = System.currentTimeMillis();
        DeferredResult<ResponseEntity<ApiResponse<QueryResponse>>> deferredResult =
                new DeferredResult<>(100L,
                        ResponseEntity.status(HttpStatus.REQUEST_TIMEOUT)
                                .body(ApiResponse.error("TIMEOUT", "Request timeout")));

        // 限流检查
        if (!tryAcquireToken(clientIp)) {
            deferredResult.setResult(
                    ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                            .body(ApiResponse.error("RATE_LIMIT", "Rate limit exceeded")));
            return deferredResult;
        }

        // 生成请求ID
        String reqId = requestId != null ? requestId : generateRequestId();
        request.setRequestId(reqId);

        // 异步处理
        binQueryService.queryAsync(request)
                .thenAccept(response -> {
                    deferredResult.setResult(
                            ResponseEntity.ok(ApiResponse.success(response))
                    );
                })
                .exceptionally(ex -> {
                    log.error("Async query failed", ex);
                    deferredResult.setErrorResult(
                            ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                                    .body(ApiResponse.error("INTERNAL_ERROR", "Internal server error"))
                    );
                    return null;
                });
        log.info("query [{}] cost: [{}] ms", request.toString(), System.currentTimeMillis() - startTime);
        return deferredResult;
    }
    @GetMapping("/queryByCard")
    public DeferredResult<ResponseEntity<ApiResponse<QueryResponse>>> query2(
            @RequestParam String cardNumber ) {
        long startTime = System.currentTimeMillis();
        DeferredResult<ResponseEntity<ApiResponse<QueryResponse>>> deferredResult =
                new DeferredResult<>(100L,
                        ResponseEntity.status(HttpStatus.REQUEST_TIMEOUT)
                                .body(ApiResponse.error("TIMEOUT", "Request timeout")));
        // 限流检查
        if (!globalBucket.tryConsume(1)) {
            deferredResult.setResult(
                    ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                            .body(ApiResponse.error("RATE_LIMIT", "Rate limit exceeded")));
            return deferredResult;
        }
        // 异步处理
        QueryRequest request = new QueryRequest(cardNumber, generateRequestId());

        binQueryService.queryAsync(request)
                .thenAccept(response -> {
                    log.info("result [{}] cost: [{}] ms",response, System.currentTimeMillis() - startTime);
                    deferredResult.setResult(
                            ResponseEntity.ok(ApiResponse.success(response))
                    );
                })
                .exceptionally(ex -> {
                    log.error("Async query failed", ex);
                    deferredResult.setErrorResult(
                            ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                                    .body(ApiResponse.error("INTERNAL_ERROR", "Internal server error"))
                    );
                    return null;
                });
        log.info(" result cost: [{}] ms", System.currentTimeMillis() - startTime);
        return deferredResult;
    }

    /**
     * 批量查询
     */
    @PostMapping("/batch-query")
    public ResponseEntity<ApiResponse<List<QueryResponse>>> batchQuery(
            @RequestBody List<QueryRequest> requests,
            @RequestHeader(value = "X-Client-IP", required = false) String clientIp) {
        log.debug("batch query :[{}]",requests.toString());
        long start = System.currentTimeMillis();
        // 限流检查
        if (!globalBucket.tryConsume(requests.size())) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .body(ApiResponse.error("RATE_LIMIT", "Global rate limit exceeded"));
        }

        try {
            List<QueryResponse> responses = binQueryService.batchQuery(requests);
            log.debug("batch query result :[{}],cost : [{}] ms",responses,System.currentTimeMillis()-start);
            return ResponseEntity.ok(ApiResponse.success(responses));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest()
                    .body(ApiResponse.error("INVALID_REQUEST", e.getMessage()));
        } catch (Exception e) {
            log.error("batch query cost [{}] ms, failed {}",System.currentTimeMillis()-start,e);
            return ResponseEntity.internalServerError()
                    .body(ApiResponse.error("INTERNAL_ERROR", "Internal server error"));
        }
    }

    /**
     * 健康检查
     */
    @GetMapping("/health")
    public ResponseEntity<ApiResponse<Map<String, Object>>> health() {
        Map<String, Object> healthInfo = binQueryService.getServiceStats();
        healthInfo.put("status", "UP");
        healthInfo.put("timestamp", System.currentTimeMillis());

        return ResponseEntity.ok(ApiResponse.success(healthInfo));
    }

    /**
     * 预热接口
     */
    @PostMapping("/preheat")
    public ResponseEntity<ApiResponse<Void>> preheat() {
        binQueryService.preheatHotData();
        return ResponseEntity.ok(ApiResponse.success(null));
    }

    /**
     * 监控指标
     */
    @GetMapping("/metrics")
    public ResponseEntity<ApiResponse<Map<String, Object>>> metrics() {
        Map<String, Object> metrics = binQueryService.getServiceStats();
        return ResponseEntity.ok(ApiResponse.success(metrics));
    }

    // 私有方法
    private boolean tryAcquireToken(String clientIp) {
        // 全局限流
        if (!globalBucket.tryConsume(1)) {
            return false;
        }
        // IP限流
        if (clientIp != null) {
            Bucket ipBucket = ipBuckets.computeIfAbsent(clientIp, ip ->
                    Bucket4j.builder()
                            .addLimit(Bandwidth.classic(100, Refill.greedy(100, Duration.ofSeconds(1))))
                            .build());

            if (!ipBucket.tryConsume(1)) {
                return false;
            }
        }

        return true;
    }

    private String generateRequestId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 16);
    }
}

