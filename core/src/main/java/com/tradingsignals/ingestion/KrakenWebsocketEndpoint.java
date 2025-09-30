package com.tradingsignals.ingestion;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;

@WebSocket(autoDemand = false)
public class KrakenWebsocketEndpoint implements Session.Listener {
    private Session session;

    @Override
    public void onWebSocketOpen(Session session){
        this.session = session;
        session.demand();
    }

    @Override
    public void onWebSocketText(String message) {
        System.out.println(message);
        session.demand();
    }

}
