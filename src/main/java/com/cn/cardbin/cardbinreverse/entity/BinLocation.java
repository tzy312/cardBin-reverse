package com.cn.cardbin.cardbinreverse.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;

//import org.springframework.data.annotation.Id;
//import org.springframework.data.redis.core.RedisHash;
//import org.springframework.data.redis.core.index.Indexed;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BinLocation implements Serializable {
    private String issuer;
    private String code;
    private String organization;
    private String province;

    public static BinLocation fromCsv(String[] fields) {
        return BinLocation.builder()
                .issuer(fields[0].trim())
                .code(fields[1].trim())
                .organization(fields[2].trim())
                .province(fields[3].trim())
                .build();
    }
}
