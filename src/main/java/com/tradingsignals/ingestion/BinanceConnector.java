package com.tradingsignals.ingestion;

import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketOpen;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.WebSocketClient;

@WebSocket(autoDemand = true)
class WebsocketListener{
    @OnWebSocketOpen
    public void onOpen(){
        System.out.println("Websocket connected");
    }

    @OnWebSocketMessage
    public void onText(String message){
        System.out.println("Got message: " + message);
    }
}


public class BinanceConnector {

    public BinanceConnector(){};

    static public void main(){
//        var websocket = new WebsocketListener();
//        websocket.
    }
}
