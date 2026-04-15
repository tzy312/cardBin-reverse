package com.cn.cardbin.cardbinreverse.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueryRequest {
    private String cardNumber;
    private String requestId;
}
