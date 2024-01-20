/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.websockets;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.websockets.WebsocketPublisher.ListenerImpl;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;

@Immutable
@BuildableStyle
public abstract class WebsocketTable {

    public static Builder builder() {
        return ImmutableWebsocketTable.builder();
    }

    // http2 options?
    // https://eclipse.dev/jetty/documentation/jetty-11/programming-guide/index.html#pg-client-websocket-connect-http2

    // partitioning?

    public abstract URI uri();

    public abstract List<String> subscribeMessages();

    @Default
    public Predicate<? super String> filter() {
        return P.INCLUDE_ALL;
    }

    /**
     * The
     * 
     * @return the processor
     */
    @Default
    public NamedObjectProcessor<? super String> processor() {
        return NamedObjectProcessor.of(ObjectProcessor.simple(Type.stringType()), "Value");
    }

    // todo: move to TableOptions, not set default here
    @Default
    public int chunkSize() {
        return ArrayBackedColumnSource.BLOCK_SIZE;
    }

    @Default
    public int skipFirstN() {
        return 0;
    }


    // public abstract boolean receiveTimestamp();

    public final Table execute() throws Exception {
        // todo: receive timestamp
        final ListenerImpl publisher = publisher().execute();
        final TableDefinition tableDef =
                TableDefinition.from(processor().columnNames(), processor().processor().outputTypes());
        final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(tableDef, publisher,
                ExecutionContext.getContext().getUpdateGraph(), UUID.randomUUID().toString(), Map.of());
        publisher.start();
        return adapter.table();
    }

    public interface Builder {

        Builder uri(URI uri);

        default Builder uri(String uri) {
            return uri(URI.create(uri));
        }

        Builder addSubscribeMessages(String element);

        Builder addSubscribeMessages(String... elements);

        Builder addAllSubscribeMessages(Iterable<String> elements);

        Builder filter(Predicate<? super String> predicate);

        Builder processor(NamedObjectProcessor<? super String> processor);

        // todo: easier way to constructed processor?
        default Builder processor(NamedObjectProcessor.Provider nop) {
            return processor(nop.named(String.class));
        }

        Builder chunkSize(int chunkSize);

        Builder skipFirstN(int skipFirstN);

        WebsocketTable build();
    }

    private WebsocketPublisher publisher() {
        return WebsocketPublisher.builder()
                .uri(uri())
                .addAllSubscribeMessages(subscribeMessages())
                .filter(filter())
                .processor(processor().processor())
                .chunkSize(chunkSize())
                .skipFirstN(skipFirstN())
                .build();
    }

    private enum P implements Predicate<String> {
        INCLUDE_ALL;

        @Override
        public boolean test(String s) {
            return true;
        }
    }

}
