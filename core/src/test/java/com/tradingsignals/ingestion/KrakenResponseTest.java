package com.tradingsignals.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class KrakenResponseTest {
    static final String SNAPSHOT_RESPONSE = "{\"channel\":\"book\",\"type\":\"snapshot\",\"data\":[{\"symbol\":\"ETH/GBP\",\"bids\":[{\"price\":3094.60,\"qty\":1.21399386},{\"price\":3094.59,\"qty\":3.00805560},{\"price\":3094.55,\"qty\":3.00809077},{\"price\":3094.41,\"qty\":3.00822915},{\"price\":3094.24,\"qty\":0.00744000},{\"price\":3094.23,\"qty\":3.00840484},{\"price\":3094.18,\"qty\":30.08448385},{\"price\":3094.12,\"qty\":3.00850871},{\"price\":3093.96,\"qty\":0.08799034},{\"price\":3093.52,\"qty\":1.17740030}],\"asks\":[{\"price\":3094.94,\"qty\":1.21385242},{\"price\":3094.95,\"qty\":3.00770571},{\"price\":3094.98,\"qty\":3.00767655},{\"price\":3095.08,\"qty\":3.00757938},{\"price\":3095.29,\"qty\":3.07697533},{\"price\":3095.53,\"qty\":3.00714812},{\"price\":3095.67,\"qty\":0.06960000},{\"price\":3095.86,\"qty\":0.52165080},{\"price\":3095.94,\"qty\":3.00674750},{\"price\":3095.98,\"qty\":1.94160000}],\"checksum\":2216566255}]}";
    static final String UPDATE_RESPONSE = "{\"channel\":\"book\",\"type\":\"update\",\"data\":[{\"symbol\":\"BTC/USD\",\"bids\":[],\"asks\":[{\"price\":113312.4,\"qty\":16.90080565}],\"checksum\":3289960256,\"timestamp\":\"2025-09-30T13:36:20.775204Z\"}]}";

    static final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testSnapshotParsing() throws JsonProcessingException {
        var result = mapper.readValue(SNAPSHOT_RESPONSE, KrakenResponse.class);

        assertEquals("book", result.channel());
        assertEquals("snapshot", result.type());

        assertNotNull(result.data());
        var firstDataEntry = result.data()[0];
        var firstBid = firstDataEntry.bids()[0];
        assertEquals(3094.6, firstBid.price());
        assertEquals(1.21399386, firstBid.qty());
    }

    @Test
    public void testUpdateParsing() throws JsonProcessingException {
        var result = mapper.readValue(UPDATE_RESPONSE, KrakenResponse.class);

        assertEquals("book", result.channel());
        assertEquals("update", result.type());

        assertNotNull(result.data());
        var data = result.data()[0];

        assertEquals(0, data.bids().length);

        var firstAsk = data.asks()[0];
        assertEquals(113312.4, firstAsk.price());
        assertEquals(16.90080565, firstAsk.qty());

        assertEquals(new BigInteger("3289960256"), data.checksum());

//        var dateFormat = new SimpleDateFormat("yyyy-mm-dd")

//        assertEquals(Timestamp.valueOf("2025-09-30T13:36:20.775204Z", ), data.timestamp());

    }

}