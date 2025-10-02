package com.tradingsignals.models;

import java.time.Instant;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Collections;

class BookSide {
    final SortedMap<Double, Double> priceQty;

    public BookSide(){
        priceQty = new TreeMap<>();
    }

    void applyUpdate(BookLevel[] updates){
        for (var levelUpdate : updates){
            if ((Math.abs(levelUpdate.qty()) < 1e-3) && priceQty.containsKey(levelUpdate.price())) {
                priceQty.remove(levelUpdate.price());
                continue;
            }

            priceQty.put(levelUpdate.price(), levelUpdate.qty());
        }
    }
}

public class OrderBook {
    int depth;
    BookSide bids;
    BookSide asks;
    Instant heartbeat;

    public enum Side {BIDS, ASKS};

    public OrderBook(int depth){
        this.depth = depth;
        bids = new BookSide();
        asks = new BookSide();
    }

    public void applyUpdate(BookLevel[] bidUpdates, BookLevel[] askUpdates){
        bids.applyUpdate(bidUpdates);
        if (bids.priceQty.size() > depth) bids.priceQty.remove(bids.priceQty.firstKey());

        asks.applyUpdate(askUpdates);
        if (asks.priceQty.size() > depth) asks.priceQty.remove(asks.priceQty.lastKey());
    }

    public SortedMap<Double, Double> getSide(Side side){
        var bookSide = (side == Side.BIDS) ? bids : asks;
        return Collections.unmodifiableSortedMap(bookSide.priceQty);
    }
}
