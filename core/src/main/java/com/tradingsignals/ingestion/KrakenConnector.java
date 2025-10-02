package com.tradingsignals.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tradingsignals.models.OrderBook;
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

    public void processMessage(KrakenResponse response){
//        if (Objects.equals(response.channel(), "heartbeat")){
//
//        }
//        logger.info(response);
    }

    static void main() throws Exception {
        var symbols = new String[]{"BTC/USD", "ETH/GBP"};
        var connector = new KrakenConnector();

        connector.toggleSubscription(true, symbols);

        new Thread(() -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.out.println("Sending unsubscribe message");
            connector.toggleSubscription(false, symbols);
        }).start();

        var processMessageThread = new Thread(() -> {
           var queue = connector.messageQueue;
           while (true){
               KrakenResponse message = null;
               try {
                   message = queue.take();
               } catch (InterruptedException e) {
                   throw new RuntimeException(e);
               }
               logger.info(message.toString());
           }
        });
        processMessageThread.start();

        Thread.sleep(10_000);

        connector.webSocketClient.close();
        processMessageThread.interrupt();
    }

}
