package com.tradingsignals.ingestion;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

@WebSocket(autoDemand = false)
public class KrakenWebsocketEndpoint implements Session.Listener {
    private static final Logger logger = LoggerFactory.getLogger(KrakenWebsocketEndpoint.class);
    private Session session;
    final BlockingQueue<KrakenResponse> messageQueue;
    ObjectMapper mapper;

    public KrakenWebsocketEndpoint(BlockingQueue<KrakenResponse> messageQueue){
        this.messageQueue = messageQueue;
    }

    @Override
    public void onWebSocketOpen(Session session){
        logger.info("Kraken websocket connection opened");
        this.session = session;
        this.mapper = new ObjectMapper();
        session.demand();
    }

    @Override
    public void onWebSocketText(String message) {
        KrakenResponse response = null;
        try {
            response = KrakenResponse.parseMessage(message);
        } catch (JsonProcessingException e) {}

        if (response != null) {
            var appendResult = messageQueue.offer(response);
            if (!appendResult) logger.warn("Message queue is at max capacity, order book update ignored");
        }

        session.demand();
    }

}
