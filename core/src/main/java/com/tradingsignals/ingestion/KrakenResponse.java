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
    static final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    @JsonIgnoreProperties(ignoreUnknown = true)
    record CoinData(
            String symbol, CoinBookLevel[] bids, CoinBookLevel[] asks, @Nullable BigInteger checksum,
            @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'", timezone = "UTC")
            @Nullable
            Instant timestamp
    ){}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record CoinBookLevel(Double price, Double qty){}

    public static KrakenResponse parseMessage(String message) throws JsonProcessingException {
        return mapper.readValue(message, KrakenResponse.class);
    }
}
