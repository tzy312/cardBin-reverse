package com.cn.cardbin.cardbinreverse.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GlobleConfig {
    @Value("${card.bin.lengths:5,6,7}")
    private String lengths;

    @Bean("binLengths")
    public int[] binLengths(){
        String[] split = lengths.split(",");
        int[] binLengths = new int[split.length];
        for (int i = 0; i < split.length; i++) {
            binLengths[i] = Integer.parseInt(split[i]);
        }
        return binLengths;
    }
}
