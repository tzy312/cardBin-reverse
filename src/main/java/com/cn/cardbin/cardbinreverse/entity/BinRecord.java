package com.cn.cardbin.cardbinreverse.entity;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
//import org.springframework.data.annotation.Id;
//import org.springframework.data.redis.core.RedisHash;
//import org.springframework.data.redis.core.index.Indexed;
import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BinRecord implements Serializable {
    private String id;
    private String prefix;
    private String issuer;
    private String cardType;
    private String country;
    private String province;
    private String city;
    private String locationCode;
    private String locationCodeLength;
    private String organization;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    public static BinRecord fromCsv(String[] fields) {
        return BinRecord.builder()
                .prefix(fields[0].trim())
                .issuer(fields[1].trim())
                .cardType(fields[2].trim())
                .country(fields[3].trim())
                .province(fields[4].trim())
                .city(fields[5].trim())
                .locationCode(fields[6].trim())
                .locationCodeLength(fields[7].trim())
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();
    }
}
