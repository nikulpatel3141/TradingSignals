package com.tradingsignals.models;

import static com.tradingsignals.ingestion.OrderBookFeatures.orderBookFeatures;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

record BookUpdate (double price, double qty) implements BookLevel {};

class OrderBookTest {

    public OrderBook getOrderBook(){
        var orderBook = new OrderBook(2);

        var bids = new BookUpdate[]{new BookUpdate(1.0, 12.123), new BookUpdate(1.1, 2.123)};
        orderBook.applyUpdate(bids, bids);

        bids = new BookUpdate[]{new BookUpdate(1.05, 1.4)};
        orderBook.applyUpdate(bids, bids);
        return orderBook;
    }

    @Test
    public void testApplyUpdate(){
        var orderBook = getOrderBook();

        // should remove the worst bid + ask
        var bookBids = orderBook.getSide(OrderBook.Side.BIDS);
        assertEquals(2, bookBids.size());
        assertEquals(1.05, bookBids.firstKey());
        assertEquals(1.1, bookBids.lastKey());

        var bookAsks = orderBook.getSide(OrderBook.Side.ASKS);
        assertEquals(2, bookAsks.size());
        assertEquals(1.0, bookAsks.firstKey());
        assertEquals(1.05, bookAsks.lastKey());
    }

    @Test
    public void testCalculateFeatures(){
        var orderBook = getOrderBook();
        var features = orderBookFeatures(orderBook);

        var expectedFeatures = new HashMap<String, Double>();
        expectedFeatures.put("bidAskSpread", 0.1);
        expectedFeatures.put("orderImbalance", 0.20668);

        assertEquals(expectedFeatures.keySet(), features.keySet());
        for (var key: expectedFeatures.keySet()){
            assertEquals(expectedFeatures.get(key), features.get(key), 0.01);
        }

    }
}
