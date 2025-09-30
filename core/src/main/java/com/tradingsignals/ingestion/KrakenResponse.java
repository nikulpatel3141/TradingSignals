package com.tradingsignals.ingestion;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.sql.Timestamp;

@JsonIgnoreProperties(ignoreUnknown = true)
public record KrakenResponse(String channel, @Nullable String type, @Nullable CoinData[] data){

    @JsonIgnoreProperties(ignoreUnknown = true)
    record CoinData(String symbol, CoinBookLevel[] bids, CoinBookLevel[] asks, @Nullable BigInteger checksum, @Nullable Timestamp timestamp){}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record CoinBookLevel(Double price, Double qty){}
}
