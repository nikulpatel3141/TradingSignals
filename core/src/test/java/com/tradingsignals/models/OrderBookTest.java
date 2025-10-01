package com.tradingsignals.models;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

record BookUpdate (double price, double qty) implements BookLevel {};

class OrderBookTest {
    @Test
    public void testApplyUpdate(){
        var initialBook = new OrderBook(2);

        var bids = new BookUpdate[]{new BookUpdate(1.0, 12.123), new BookUpdate(1.1, 2.123)};
        initialBook.applyUpdate(bids, bids);

        bids = new BookUpdate[]{new BookUpdate(1.05, 1.4)};
        initialBook.applyUpdate(bids, bids);

        // should remove the worst bid + ask
        var bookBids = initialBook.getSide(OrderBook.Side.BIDS);
        assertEquals(2, bookBids.size());
        assertEquals(1.05, bookBids.firstKey());
        assertEquals(1.1, bookBids.lastKey());

        var bookAsks = initialBook.getSide(OrderBook.Side.ASKS);
        assertEquals(2, bookAsks.size());
        assertEquals(1.0, bookAsks.firstKey());
        assertEquals(1.05, bookAsks.lastKey());
    }
}
