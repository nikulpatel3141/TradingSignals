package com.tradingsignals.ingestion;

import com.tradingsignals.models.OrderBook;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shaded.parquet.it.unimi.dsi.fastutil.Pair;
import tradingsignals.ingestion.OrderBookFeaturesSchema;

import java.io.ByteArrayOutputStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public final class OrderBookFeatures {
    private static final Logger logger = LoggerFactory.getLogger(OrderBookFeatures.class);

    private OrderBookFeatures(){}

    public static HashMap<String, Double> orderBookFeatures(OrderBook book){
        var bids = book.getSide(OrderBook.Side.BIDS);
        var asks = book.getSide(OrderBook.Side.ASKS);

//        var midpoint = (bids.lastKey() + asks.firstKey()) / 2;
        var bidAskSpread = bids.lastKey() - asks.firstKey();

        var bidVolume = bids.values().stream().reduce(0.0, Double::sum);
        var askVolume = asks.values().stream().reduce(0.0, Double::sum);

        var orderImbalance = bidVolume / (bidVolume + askVolume);

        HashMap<String, Double> features = new HashMap<>();
        features.put("bidAskSpread", bidAskSpread);
        features.put("orderImbalance", orderImbalance);

        return features;
    }

    public static byte[] SerialiseFeatures(Map<String, OrderBook> bookData){
        var schema = OrderBookFeaturesSchema.getClassSchema();
        DatumWriter<OrderBookFeaturesSchema> writer = new SpecificDatumWriter<>(schema);
        byte[] data = new byte[0];

        var stream = new ByteArrayOutputStream();
        try {
            var encoder = EncoderFactory.get().jsonEncoder(schema, stream);
            for (var entry : bookData.entrySet()){
                var book = entry.getValue();
                var features = orderBookFeatures(book);
                var featuresAvro = new OrderBookFeaturesSchema(entry.getKey(), book.updateTime, features.get("bidAskSpread"), features.get("orderImbalance"));
                writer.write(featuresAvro, encoder);
            }
        } catch (Exception e){
            logger.error("Serialisation error: {}", e.getMessage());
        }

        return data;
    }
}
