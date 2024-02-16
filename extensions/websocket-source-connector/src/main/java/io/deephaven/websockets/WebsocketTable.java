/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.websockets;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.json.Processor;
import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.websockets.WebsocketPublisher.WebsocketStreamPublisher;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import javax.annotation.Nullable;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class WebsocketTable {

    public static Builder builder() {
        return ImmutableWebsocketTable.builder();
    }

    // http2 options?
    // https://eclipse.dev/jetty/documentation/jetty-11/programming-guide/index.html#pg-client-websocket-connect-http2

    // partitioning?

    /**
     * The URI to connect to. Must have a host and a scheme of "ws" or "wss".
     *
     * @return the uri
     */
    public abstract URI uri();

    /**
     *
     * @return
     */
    public abstract List<String> subscribeMessages(); // todo: binary support

    public abstract Optional<Predicate<? super String>> stringFilter();

    public abstract Optional<Predicate<? super byte[]>> bytesFilter();

    public abstract Optional<Predicate<? super ByteBuffer>> byteBufferFilter();

    public abstract Optional<NamedObjectProcessor<? super String>> stringProcessor();

    public abstract Optional<NamedObjectProcessor<? super byte[]>> bytesProcessor();

    public abstract Optional<NamedObjectProcessor<? super ByteBuffer>> byteBufferProcessor();

    /**
     *
     * @return
     */
    @Default
    public int chunkSize() {
        return ArrayBackedColumnSource.BLOCK_SIZE;
    }

    /**
     * The number of initial messages responses to skip. This may be useful in situations where an initial connect or
     * subscription message produces a response that is not necessary to process. By default, is {@code 0}.
     *
     * @return the number of initial messages to skip
     */
    @Default
    public int skipFirstN() {
        return 0;
    }

    /**
     * The receive timestamp column name. By default, is "ReceiveTimestamp". When set to {@code null}, the resulting
     * table will not contain the receive timestamp column.
     *
     * @return the receive timestamp colum name
     */
    @Default
    @Nullable
    public String receiveTimestamp() {
        return "ReceiveTimestamp";
    }

    // ---------------------------------------------------------------------

    @Default
    public String name() {
        return UUID.randomUUID().toString();
    }

    /**
     * The update source registrar. By default, is equivalent to {@code ExecutionContext.getContext().getUpdateGraph()}.
     *
     * @return the update source registrar
     */
    @Default
    public UpdateSourceRegistrar updateSourceRegistrar() {
        return ExecutionContext.getContext().getUpdateGraph();
    }

    /**
     * The extra attributes to set on the blink table.
     *
     * @return the extra attributes
     */
    public abstract Map<String, Object> extraAttributes();

    // ---------------------------------------------------------------------

    public final TableDefinition tableDefinition() {
        return TableDefinition.from(columnNames(), columnTypes());
    }

    public final Table execute() throws Exception {
        final WebsocketStreamPublisher publisher = publisher().execute();
        final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(tableDefinition(), publisher,
                updateSourceRegistrar(), name(), extraAttributes());
        publisher.start();
        return adapter.table();
    }

    // ---------------------------------------------------------------------

    public interface Builder {

        Builder uri(URI uri);

        default Builder uri(String uri) {
            return uri(URI.create(uri));
        }

        Builder addSubscribeMessages(String element);

        Builder addSubscribeMessages(String... elements);

        Builder addAllSubscribeMessages(Iterable<String> elements);

        Builder stringFilter(Predicate<? super String> stringFilter);

        Builder bytesFilter(Predicate<? super byte[]> bytesFilter);

        Builder byteBufferFilter(Predicate<? super ByteBuffer> byteBufferFilter);

        Builder stringProcessor(NamedObjectProcessor<? super String> stringProcessor);

        default Builder stringProcessor(NamedObjectProcessor.Provider stringProcessor) {
            return stringProcessor(stringProcessor.named(String.class));
        }

        Builder bytesProcessor(NamedObjectProcessor<? super byte[]> stringProcessor);

        default Builder bytesProcessor(NamedObjectProcessor.Provider bytesProcessor) {
            return bytesProcessor(bytesProcessor.named(byte[].class));
        }

        Builder byteBufferProcessor(NamedObjectProcessor<? super ByteBuffer> stringProcessor);

        default Builder byteBufferProcessor(NamedObjectProcessor.Provider bytesProcessor) {
            return byteBufferProcessor(bytesProcessor.named(ByteBuffer.class));
        }

        Builder chunkSize(int chunkSize);

        Builder skipFirstN(int skipFirstN);

        Builder receiveTimestamp(String receiveTimestamp);

        WebsocketTable build();
    }

    // ---------------------------------------------------------------------

    @Check
    final void checkUri() {
        WebsocketPublisher.checkUri(uri());
    }

    @Check
    final void checkChunkSize() {
        WebsocketPublisher.checkChunkSize(chunkSize());
    }

    @Check
    final void checkSkipFirstN() {
        WebsocketPublisher.checkSkipFirstN(skipFirstN());
    }

    @Check
    final void checkZeroOrOneOf() {
        final int count = (stringProcessor().isPresent() ? 1 : 0)
                + (bytesProcessor().isPresent() ? 1 : 0)
                + (byteBufferProcessor().isPresent() ? 1 : 0);
        if (count > 1) {
            throw new IllegalArgumentException(
                    "Must have exactly at most one of stringProcessor, bytesProcessor, or byteBufferProcessor");
        }
    }

    private Optional<NamedObjectProcessor<? super String>> defaultStringProcessor() {
        if (stringProcessor().isPresent() || bytesProcessor().isPresent() || byteBufferProcessor().isPresent()) {
            return Optional.empty();
        }
        return Optional.of(NamedObjectProcessor.of(ObjectProcessor.simple(Type.stringType()), "Value"));
    }

    private NamedObjectProcessor<?> processor() {
        return Stream.of(stringProcessor(), bytesProcessor(), byteBufferProcessor(), defaultStringProcessor())
                .flatMap(Optional::stream)
                .findFirst()
                .orElseThrow();
    }

    private List<String> columnNames() {
        return Stream.concat(
                Stream.ofNullable(receiveTimestamp()),
                processor().columnNames().stream())
                .collect(Collectors.toList());
    }

    private List<Type<?>> columnTypes() {
        return Stream.concat(
                receiveTimestamp() == null ? Stream.empty() : Stream.of(Type.instantType()),
                processor().processor().outputTypes().stream())
                .collect(Collectors.toList());
    }

    private WebsocketPublisher publisher() {
        final WebsocketPublisher.Builder builder = WebsocketPublisher.builder()
                .uri(uri())
                .addAllSubscribeMessages(subscribeMessages())
                .chunkSize(chunkSize())
                .skipFirstN(skipFirstN())
                .receiveTimestamp(receiveTimestamp() != null);

        stringFilter().ifPresent(builder::stringFilter);
        bytesFilter().ifPresent(builder::bytesFilter);
        byteBufferFilter().ifPresent(builder::byteBufferFilter);

        stringProcessor().map(NamedObjectProcessor::processor).ifPresent(builder::stringProcessor);
        bytesProcessor().map(NamedObjectProcessor::processor).ifPresent(builder::bytesProcessor);
        byteBufferProcessor().map(NamedObjectProcessor::processor).ifPresent(builder::byteBufferProcessor);
        defaultStringProcessor().map(NamedObjectProcessor::processor).ifPresent(builder::stringProcessor);

        return builder.build();
    }
}
