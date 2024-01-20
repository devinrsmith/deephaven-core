/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.websockets;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.eclipse.jetty.websocket.api.WriteCallback;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.immutables.value.Value.Immutable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

@Immutable
@BuildableStyle
public abstract class WebsocketPublisher {

    public static Builder builder() {
        return ImmutableWebsocketPublisher.builder();
    }

    // http2 options?
    // https://eclipse.dev/jetty/documentation/jetty-11/programming-guide/index.html#pg-client-websocket-connect-http2

    // partitioning?

    public abstract URI uri();

    public abstract List<String> subscribeMessages();

    public abstract Predicate<? super String> filter();

    public abstract ObjectProcessor<? super String> processor();

    public abstract int chunkSize();

    public abstract int skipFirstN();


    // public abstract boolean receiveTimestamp();

    // todo: remove this

    final ListenerImpl execute() {
        return publisher();
    }

    public interface Builder {

        Builder uri(URI uri);

        Builder addSubscribeMessages(String element);

        Builder addSubscribeMessages(String... elements);

        Builder addAllSubscribeMessages(Iterable<String> elements);

        Builder filter(Predicate<? super String> predicate);

        Builder processor(ObjectProcessor<? super String> processor);

        Builder chunkSize(int chunkSize);

        Builder skipFirstN(int skipFirstN);

        WebsocketPublisher build();
    }

    final ListenerImpl publisher() {
        return new ListenerImpl();
    }

    class ListenerImpl implements WebSocketListener, StreamPublisher {

        private final WritableObjectChunk<String, ?> buffer;
        private WritableChunk<Values>[] chunks;
        private StreamConsumer consumer;
        private int skipped = 0;

        private WebSocketClient client;
        private Session session;

        public ListenerImpl() {
            this.buffer = WritableObjectChunk.makeWritableChunk(chunkSize());
            this.buffer.setSize(0);
            this.chunks = newProcessorChunks();
        }

        void start() throws Exception {
            client = new WebSocketClient();
            client.start();
            // todo: do we get all errors through WebSocketListener, or do we need to look at error from future?
            session = client.connect(this, uri()).get();
            for (String subscribeMessage : subscribeMessages()) {
                session.getRemote().sendString(subscribeMessage, new WriteCallback() {
                    @Override
                    public void writeFailed(Throwable x) {
                        consumer.acceptFailure(new IOException("Error sending subscribe message", x));
                    }
                });
            }
        }

        // -----------------------------------------------

        @Override
        public void register(@NotNull StreamConsumer consumer) {
            this.consumer = consumer;
        }

        @Override
        public synchronized void flush() {
            if (buffer.size() != 0) {
                flushImpl();
            }
        }

        @Override
        public void shutdown() {
            // System.out.println("shutting down");
            session.close(StatusCode.NORMAL, null);
        }

        // -----------------------------------------------

        @Override
        public void onWebSocketConnect(Session session) {

        }

        @Override
        public void onWebSocketError(Throwable cause) {
            consumer.acceptFailure(cause);
        }

        @Override
        public void onWebSocketText(String message) {
            // todo: guaranteed to be happens-before semantics (thread safe?)
            if (skipped != skipFirstN()) {
                ++skipped;
                return;
            }
            if (!filter().test(message)) {
                return;
            }
            synchronized (this) {
                buffer.add(message);
                if (buffer.size() == chunkSize()) {
                    flushImpl();
                }
            }
        }

        @Override
        public void onWebSocketClose(int statusCode, String reason) {
            consumer.acceptFailure(new IOException("Websocket closed: " + statusCode + " " + reason));
        }

        @Override
        public void onWebSocketBinary(byte[] payload, int offset, int len) {
            session.close(StatusCode.BAD_DATA, "Received unexpected binary data");
            consumer.acceptFailure(new IOException("Received unexpected binary data"));
        }

        // -----------------------------------------------

        private void flushImpl() {
            // todo: ordering
            processor().processAll(buffer, Arrays.asList(chunks));
            buffer.fillWithNullValue(0, buffer.size());
            buffer.setSize(0);
            consumer.accept(chunks);
            chunks = newProcessorChunks();
        }

        private WritableChunk<Values>[] newProcessorChunks() {
            // noinspection unchecked
            return processor()
                    .outputTypes()
                    .stream()
                    .map(ObjectProcessor::chunkType)
                    .map(chunkType -> chunkType.makeWritableChunk(chunkSize()))
                    .peek(wc -> wc.setSize(0))
                    .toArray(WritableChunk[]::new);
        }
    }
}
