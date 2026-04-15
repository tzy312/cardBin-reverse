package com.cn.cardbin.cardbinreverse.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BinInfo {
    private String prefix;
    private String issuer;
    private String cardType;
    private String country;
    private String province;
    private String city;
    private String organization;
    /*private String countryCode;
    private String currency;
    private String network;
    private String bankCode;*/
}
