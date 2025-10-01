package com.tradingsignals.ingestion;

import com.tradingsignals.models.OrderBook;

import java.util.HashMap;

public final class OrderBookFeatures {
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

}
