package com.tradingsignals.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradingsignals.models.OrderBook;
import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Instant;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


record SubscribeMessage(String method, SubscribeParams params){
    record SubscribeParams(String channel, String[] symbol){}
}

/**
 * See <a href="https://docs.kraken.com/api/docs/websocket-v2/book">...</a>
 */
public class KrakenConnector {
    private static final Logger logger = LoggerFactory.getLogger(KrakenConnector.class);
    static final String KRAKEN_WEBSOCKET_URL = "wss://ws.kraken.com/v2";

    boolean subscribed = false;
    final WebSocketClient webSocketClient;
    final Session webSocketSession;
    final BlockingQueue<KrakenResponse> messageQueue;

    int depth = 10;  // hardcoded default from Kraken
    HashMap<String, OrderBook> orderBooks;

    public KrakenConnector() throws Exception {
        webSocketClient = new WebSocketClient();
        webSocketClient.start();

        messageQueue = new ArrayBlockingQueue<>(1000);

        var connectResponse = webSocketClient.connect(new KrakenWebsocketEndpoint(messageQueue), URI.create(KRAKEN_WEBSOCKET_URL));
        webSocketSession = connectResponse.join();
    }

    public void sendMessage(Object object) {
        var mapper = new ObjectMapper();
        String message;
        try {
            message = mapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        webSocketSession.sendText(message, Callback.NOOP);
    }

    public void toggleSubscription(Boolean subscribe, String[] symbols){
        var message = new SubscribeMessage(subscribe ? "subscribe" : "unsubscribe", new SubscribeMessage.SubscribeParams("book", symbols));
        sendMessage(message);
        subscribed = subscribe;
    }

    public void closeWebsocket(){
//        this.webSocketClient.close();
        new Thread(() -> LifeCycle.stop(webSocketClient)).start();
    }

    public void processMessage(KrakenResponse response) {
        var channel = response.channel();
        if (Objects.equals(channel, "heartbeat")) {
            return;
        }

        if (!channel.equals("book") || (response.data() == null)) return;

        for (var coinData : response.data()) {
            var coin = coinData.symbol();

            if (!this.orderBooks.containsKey(coin)) {
                this.orderBooks.put(coin, new OrderBook(depth));
            }
            var book = this.orderBooks.get(coin);
            book.applyUpdate(coinData.bids(), coinData.asks());
        }
    }

    public void publishMessage(){

    }

    static void main() throws Exception {
        var symbols = new String[]{"BTC/USD", "ETH/GBP"};
        var connector = new KrakenConnector();

        connector.toggleSubscription(true, symbols);

        new Thread(() -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                return;
            }
            System.out.println("Sending unsubscribe message");
            connector.toggleSubscription(false, symbols);
        }).start();

        var processMessageThread = new Thread(() -> {
           var queue = connector.messageQueue;
           while (true){
               KrakenResponse message;
               try {
                   message = queue.take();
               } catch (InterruptedException e) {
                   return;
               }
               logger.info(message.toString());
               connector.processMessage(message);
           }
        });
        processMessageThread.start();

        var publishMessageThread = new Thread(() -> {
            while (true) {
                connector.publishMessage();

                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        publishMessageThread.start();

        Thread.sleep(10_000);

        connector.closeWebsocket();
        processMessageThread.interrupt();
    }

}
