package com.tradingsignals.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@WebSocket(autoDemand = false)
public class KrakenWebsocketEndpoint implements Session.Listener {
    private static Logger logger = LoggerFactory.getLogger(KrakenWebsocketEndpoint.class);
    private Session session;
    ObjectMapper mapper;

    @Override
    public void onWebSocketOpen(Session session){
        logger.info("Kraken websocket connection opened");
        this.session = session;
        this.mapper = new ObjectMapper();
        session.demand();
    }

    @Override
    public void onWebSocketText(String message) {
        try {
            logger.info(message);
            var parsedMessage = mapper.readValue(message, KrakenResponse.class);
        } catch (JsonProcessingException e) {
            var x = 1;
        }

        session.demand();
    }

}
