package com.cn.cardbin.cardbinreverse.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueryResponse {
    private String requestId;
    private String cardNumber;
    private BinInfo binInfo;
    private Boolean cacheHit;
    private Long responseTimeNs;
}
