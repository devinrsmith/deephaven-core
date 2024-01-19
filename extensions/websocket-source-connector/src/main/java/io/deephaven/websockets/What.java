/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.websockets;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.eclipse.jetty.websocket.client.WebSocketClient;

import java.net.URI;
import java.time.Duration;

public class What {

    static class MyEndpoint extends WebSocketAdapter {
        @Override
        public void onWebSocketBinary(byte[] payload, int offset, int len) {
            super.onWebSocketBinary(payload, offset, len);
        }

        @Override
        public void onWebSocketClose(int statusCode, String reason) {
            super.onWebSocketClose(statusCode, reason);
        }

        @Override
        public void onWebSocketConnect(Session sess) {
            sess.setMaxTextMessageSize(16 * 1024);
            sess.setIdleTimeout(Duration.ofSeconds(30));
        }

        @Override
        public void onWebSocketError(Throwable cause) {
            cause.printStackTrace();
        }

        @Override
        public void onWebSocketText(String message) {

            // destructure
            System.out.println(message);
        }
    }

    static void test() throws Exception {

        final String test = "{type: subscribe, productIds: [BTC-USD], channels: [ticker]}";

        final WebSocketClient client = new WebSocketClient();
        try {
            client.start();
            try (final Session session =
                    client.connect(new MyEndpoint(), URI.create("wss://ws-feed.exchange.coinbase.com")).get()) {
                session.getRemote().sendString("{\n" +
                        "      \"type\": \"subscribe\",\n" +
                        "      \"product_ids\": [\n" +
                        "        \"BTC-USD\"\n" +
                        "      ],\n" +
                        "      \"channels\": [\"ticker\"]\n" +
                        "    }");

                Thread.sleep(10000);
            }
        } finally {
            client.stop();
        }
    }

    public static void main(String[] args) throws Exception {
        test();
    }
}
