/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.websockets;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.base.clock.Clock;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.http.HttpScheme;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.eclipse.jetty.websocket.api.WriteCallback;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

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

    public abstract Optional<Predicate<? super String>> stringFilter();

    public abstract Optional<Predicate<? super byte[]>> bytesFilter();

    public abstract Optional<Predicate<? super ByteBuffer>> byteBufferFilter();

    public abstract Optional<ObjectProcessor<? super String>> stringProcessor();

    public abstract Optional<ObjectProcessor<? super byte[]>> bytesProcessor();

    public abstract Optional<ObjectProcessor<? super ByteBuffer>> byteBufferProcessor();

    public abstract int chunkSize();

    public abstract int skipFirstN();

    public abstract boolean receiveTimestamp();

    public interface Builder {

        Builder uri(URI uri);

        Builder addSubscribeMessages(String element);

        Builder addSubscribeMessages(String... elements);

        Builder addAllSubscribeMessages(Iterable<String> elements);

        Builder stringFilter(Predicate<? super String> stringFilter);

        Builder bytesFilter(Predicate<? super byte[]> bytesFilter);

        Builder byteBufferFilter(Predicate<? super ByteBuffer> byteBufferFilter);

        Builder stringProcessor(ObjectProcessor<? super String> stringProcessor);

        Builder bytesProcessor(ObjectProcessor<? super byte[]> bytesProcessor);

        Builder byteBufferProcessor(ObjectProcessor<? super ByteBuffer> byteBufferProcessor);

        Builder chunkSize(int chunkSize);

        Builder skipFirstN(int skipFirstN);

        Builder receiveTimestamp(boolean receiveTimestamp);

        WebsocketPublisher build();
    }

    // todo: remove this

    final WebsocketStreamPublisher execute() {
        return publisher();
    }

    final WebsocketStreamPublisher publisher() {
        return new WebsocketStreamPublisher();
    }

    @Check
    final void checkUri() {
        checkUri(uri());
    }

    @Check
    final void checkChunkSize() {
        checkChunkSize(chunkSize());
    }

    @Check
    final void checkSkipFirstN() {
        checkSkipFirstN(skipFirstN());
    }

    @Check
    final void checkString() {
        if (stringFilter().isPresent() && stringProcessor().isEmpty()) {
            throw new IllegalArgumentException();
        }
    }

    @Check
    final void checkBytes() {
        if (bytesFilter().isPresent() && bytesProcessor().isEmpty()) {
            throw new IllegalArgumentException();
        }
    }

    @Check
    final void checkByteBuffer() {
        if (byteBufferFilter().isPresent() && byteBufferProcessor().isEmpty()) {
            throw new IllegalArgumentException();
        }
    }

    @Check
    final void checkOneOf() {
        final int count = (stringProcessor().isPresent() ? 1 : 0)
                + (bytesProcessor().isPresent() ? 1 : 0)
                + (byteBufferProcessor().isPresent() ? 1 : 0);
        if (count != 1) {
            throw new IllegalArgumentException(
                    "Must have exactly one stringProcessor, bytesProcessor, or byteBufferProcessor");
        }
    }

    static void checkUri(URI uri) {
        if (!uri.isAbsolute()) {
            throw new IllegalArgumentException("WebSocket URI must be absolute");
        }
        final String scheme = uri.getScheme();
        if (!HttpScheme.WS.is(scheme) && !HttpScheme.WSS.is(scheme)) {
            throw new IllegalArgumentException(String.format("Must use scheme '%s' or '%s', '%s' not supported",
                    HttpScheme.WS.asString(), HttpScheme.WSS.asString(), scheme));
        }
        if (uri.getHost() == null) {
            throw new IllegalArgumentException("Invalid WebSocket URI: host not present");
        }
    }

    static void checkChunkSize(int chunkSize) {
        if (chunkSize < 1) {
            throw new IllegalArgumentException("chunkSize must be positive");
        }
    }

    static void checkSkipFirstN(int skipFirstN) {
        if (skipFirstN < 0) {
            throw new IllegalArgumentException("skipFirstN must be non-negative");
        }
    }

    class WebsocketStreamPublisher implements WebSocketListener, StreamPublisher {

        private final WritableObjectChunk<String, ?> stringBuffer;
        private final WritableObjectChunk<byte[], ?> bytesBuffer;
        private final WritableObjectChunk<ByteBuffer, ?> byteBufferBuffer;

        private WritableChunk<Values>[] chunks;
        private WritableLongChunk<Values> receiveTimestampChunk;
        private StreamConsumer consumer;
        private int skipped = 0;
        private WebSocketClient client;
        private Session session;

        public WebsocketStreamPublisher() {
            if (stringProcessor().isPresent()) {
                stringBuffer = WritableObjectChunk.makeWritableChunk(chunkSize());
                stringBuffer.setSize(0);
                bytesBuffer = null;
                byteBufferBuffer = null;
                chunks = newChunks(stringProcessor().get());
            } else if (bytesProcessor().isPresent()) {
                stringBuffer = null;
                bytesBuffer = WritableObjectChunk.makeWritableChunk(chunkSize());
                bytesBuffer.setSize(0);
                byteBufferBuffer = null;
                chunks = newChunks(bytesProcessor().get());
            } else if (byteBufferProcessor().isPresent()) {
                stringBuffer = null;
                bytesBuffer = null;
                byteBufferBuffer = WritableObjectChunk.makeWritableChunk(chunkSize());
                byteBufferBuffer.setSize(0);
                chunks = newChunks(byteBufferProcessor().get());
            } else {
                throw new IllegalStateException();
            }
        }

        public void start() throws Exception {
            final HttpClient httpClient = new HttpClient();
            // todo: http config (ie, proxy?)
            client = new WebSocketClient(httpClient);
            // todo: websocket policy config org.eclipse.jetty.websocket.api.WebSocketPolicy
            client.start();
            // todo: do we get all errors through WebSocketListener, or do we need to look at error from future?
            // https://eclipse.dev/jetty/documentation/jetty-11/programming-guide/index.html#pg-client-websocket-connect-custom-http-request
            final ClientUpgradeRequest request = null;
            // todo: offer custom headers, cookies? Or, just expose ClientUpgradeRequest?
            session = client.connect(this, uri(), request).get();
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
            if (this.consumer != null) {
                throw new IllegalStateException();
            }
            this.consumer = Objects.requireNonNull(consumer);
        }

        @Override
        public synchronized void flush() {
            if (stringBuffer != null) {
                if (stringBuffer.size() != 0) {
                    stringFlushImpl();
                }
                return;
            }
            if (bytesBuffer != null) {
                if (bytesBuffer.size() != 0) {
                    bytesFlushImpl();
                }
                return;
            }
            if (byteBufferBuffer != null) {
                if (byteBufferBuffer.size() != 0) {
                    byteBufferFlushImpl();
                }
                return;
            }
            throw new IllegalStateException();
        }

        @Override
        public void shutdown() {
            System.out.println("shutting down ");
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
        public void onWebSocketBinary(byte[] payload, int offset, int len) {
            final long currentTimeNanos = receiveTimestamp() ? Clock.system().currentTimeNanos() : 0;
            if (byteBufferBuffer != null) {
                onBinaryByteBufferProcessor(currentTimeNanos, payload, offset, len);
                return;
            }
            if (bytesBuffer != null) {
                onBinaryBytesProcessor(currentTimeNanos, payload, offset, len);
                return;
            }
            session.close(StatusCode.BAD_DATA, "Received unexpected binary data");
            consumer.acceptFailure(new IOException("Received unexpected binary data"));
        }

        @Override
        public void onWebSocketText(String message) {
            final long currentTimeNanos = receiveTimestamp() ? Clock.system().currentTimeNanos() : 0;
            if (stringBuffer != null) {
                onTextStringProcessor(currentTimeNanos, message);
                return;
            }
            session.close(StatusCode.BAD_DATA, "Received unexpected text data");
            consumer.acceptFailure(new IOException("Received unexpected text data"));
        }

        @Override
        public void onWebSocketClose(int statusCode, String reason) {
            consumer.acceptFailure(new IOException("Websocket closed: " + statusCode + " " + reason));
        }

        // -----------------------------------------------

        private void onTextStringProcessor(final long currentTimeNanos, final String message) {
            if (skipped != skipFirstN()) {
                ++skipped;
                return;
            }
            if (!stringFilter().orElse(P.INCLUDE_ALL).test(message)) {
                return;
            }
            synchronized (this) {
                if (receiveTimestamp()) {
                    receiveTimestampChunk.add(currentTimeNanos);
                }
                stringBuffer.add(message);
                if (stringBuffer.size() == chunkSize()) {
                    stringFlushImpl();
                }
            }
        }

        private void onBinaryByteBufferProcessor(final long currentTimeNanos, byte[] payload, int offset, int len) {
            if (skipped != skipFirstN()) {
                ++skipped;
                return;
            }
            final ByteBuffer message = ByteBuffer.wrap(payload, offset, len);
            if (!byteBufferFilter().orElse(P.INCLUDE_ALL).test(message)) {
                return;
            }
            synchronized (this) {
                if (receiveTimestamp()) {
                    receiveTimestampChunk.add(currentTimeNanos);
                }
                byteBufferBuffer.add(message);
                if (byteBufferBuffer.size() == chunkSize()) {
                    byteBufferFlushImpl();
                }
            }
        }

        private void onBinaryBytesProcessor(final long currentTimeNanos, byte[] payload, int offset, int len) {
            if (skipped != skipFirstN()) {
                ++skipped;
                return;
            }
            final byte[] message;
            if (offset == 0 && len == payload.length) {
                message = payload;
            } else {
                message = new byte[len];
                System.arraycopy(payload, offset, message, 0, len);
            }
            if (!bytesFilter().orElse(P.INCLUDE_ALL).test(message)) {
                return;
            }
            synchronized (this) {
                if (receiveTimestamp()) {
                    receiveTimestampChunk.add(currentTimeNanos);
                }
                bytesBuffer.add(message);
                if (bytesBuffer.size() == chunkSize()) {
                    bytesFlushImpl();
                }
            }
        }

        private void stringFlushImpl() {
            // todo: ordering
            final ObjectProcessor<? super String> processor = stringProcessor().orElseThrow();
            processor.processAll(stringBuffer, processorChunks());
            stringBuffer.fillWithNullValue(0, stringBuffer.size());
            stringBuffer.setSize(0);
            consumer.accept(chunks);
            chunks = newChunks(processor);
        }

        private void bytesFlushImpl() {
            // todo: ordering
            final ObjectProcessor<? super byte[]> processor = bytesProcessor().orElseThrow();
            processor.processAll(bytesBuffer, processorChunks());
            bytesBuffer.fillWithNullValue(0, bytesBuffer.size());
            bytesBuffer.setSize(0);
            consumer.accept(chunks);
            chunks = newChunks(processor);
        }

        private void byteBufferFlushImpl() {
            // todo: ordering
            final ObjectProcessor<? super ByteBuffer> processor = byteBufferProcessor().orElseThrow();
            processor.processAll(byteBufferBuffer, processorChunks());
            byteBufferBuffer.fillWithNullValue(0, byteBufferBuffer.size());
            byteBufferBuffer.setSize(0);
            consumer.accept(chunks);
            chunks = newChunks(processor);
        }

        private List<WritableChunk<?>> processorChunks() {
            return receiveTimestamp()
                    ? Arrays.<WritableChunk<?>>asList(chunks).subList(1, chunks.length)
                    : Arrays.asList(chunks);
        }

        private WritableChunk<Values>[] newChunks(ObjectProcessor<?> processor) {
            // noinspection unchecked
            return Stream.concat(
                    receiveTimestamp()
                            ? Stream.of(receiveTimestampChunk = WritableLongChunk.makeWritableChunk(chunkSize()))
                            : Stream.empty(),
                    processor
                            .outputTypes()
                            .stream()
                            .map(ObjectProcessor::chunkType)
                            .map(chunkType -> chunkType.makeWritableChunk(chunkSize())))
                    .peek(wc -> wc.setSize(0))
                    .toArray(WritableChunk[]::new);
        }
    }

    enum P implements Predicate<Object> {
        INCLUDE_ALL;

        @Override
        public boolean test(Object s) {
            return true;
        }
    }
}
