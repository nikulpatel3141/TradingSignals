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
        initialBook.applyUpdate(bids, new BookUpdate[]{});

        bids = new BookUpdate[]{new BookUpdate(1.05, 1.4)};
        initialBook.applyUpdate(bids, new BookUpdate[]{});

        // should remove the worst bid
        var bookBids = initialBook.getSide(OrderBook.Side.BIDS);

        assertEquals(2, bookBids.size());
        assertEquals(1.05, bookBids.firstKey());
        assertEquals(1.1, bookBids.lastKey());
    }
}
