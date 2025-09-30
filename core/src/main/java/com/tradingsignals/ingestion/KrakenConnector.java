package com.tradingsignals.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.WebSocketClient;

import java.net.URI;


record SubscribeMessage(String method, SubscribeParams params){
    record SubscribeParams(String channel, String[] symbol){}
}

/**
 * See <a href="https://docs.kraken.com/api/docs/websocket-v2/book">...</a>
 */
public class KrakenConnector {
    static final String KRAKEN_WEBSOCKET_URL = "wss://ws.kraken.com/v2";

    final WebSocketClient webSocketClient;
    final Session webSocketSession;

    public KrakenConnector() throws Exception {
        webSocketClient = new WebSocketClient();
        webSocketClient.start();

        var connectResponse = webSocketClient.connect(new KrakenWebsocketEndpoint(), URI.create(KRAKEN_WEBSOCKET_URL));
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

        Thread.sleep(10_000);

        connector.webSocketClient.close();
    }

}
