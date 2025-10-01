package com.tradingsignals.ingestion;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true)
public record KrakenResponse(String channel, @Nullable String type, @Nullable CoinData[] data){
    static final ObjectMapper mapper = new ObjectMapper()
            .findAndRegisterModules();  //  https://github.com/FasterXML/jackson-modules-java8?tab=readme-ov-file#registering-modules

    record CoinData(String symbol, CoinBookLevel[] bids, CoinBookLevel[] asks, @Nullable BigInteger checksum, @Nullable Instant timestamp){}

    record CoinBookLevel(Double price, Double qty){}

    public static KrakenResponse parseMessage(String message) throws JsonProcessingException {
        return mapper.readValue(message, KrakenResponse.class);
    }
}
