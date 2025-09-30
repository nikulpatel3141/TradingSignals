package com.tradingsignals.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;

@WebSocket(autoDemand = false)
public class KrakenWebsocketEndpoint implements Session.Listener {
    private Session session;
    ObjectMapper mapper;

    @Override
    public void onWebSocketOpen(Session session){
        this.session = session;
        this.mapper = new ObjectMapper();
        session.demand();
    }

    @Override
    public void onWebSocketText(String message) {
        try {
            System.out.println(message);
            var parsedMessage = mapper.readValue(message, KrakenResponse.class);
            System.out.println(parsedMessage);
        } catch (JsonProcessingException e) {
            var x = 1;
        }

        session.demand();
    }

}
