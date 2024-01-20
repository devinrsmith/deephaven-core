/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.websockets;

import com.fasterxml.jackson.core.JsonFactory;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.json.ObjectProcessorJsonValueFromString;
import io.deephaven.json.ValueOptions;
import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.websockets.WebsocketOptions.ListenerImpl;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;

@Immutable
@BuildableStyle
public abstract class WebsocketTableOptions {

    public static Builder builder() {
        return ImmutableWebsocketTableOptions.builder();
    }

    // http2 options?
    // https://eclipse.dev/jetty/documentation/jetty-11/programming-guide/index.html#pg-client-websocket-connect-http2

    // partitioning?

    public abstract URI uri();

    public abstract List<String> subscribeMessage();

    @Default
    public Predicate<String> filter() {
        return x -> true;
    }

    /**
     * The
     * 
     * @return the processor
     */
    @Default
    public NamedObjectProcessor<String> processor() {
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
        final ListenerImpl publisher = WebsocketOptions.builder()
                .uri(uri())
                .filter(filter())
                .processor(processor())
                .chunkSize(chunkSize())
                .skipFirstN(skipFirstN())
                .build()
                .execute();
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

        Builder subscribeMessage(String subscribeMessage);

        Builder filter(Predicate<String> predicate);

        Builder processor(NamedObjectProcessor<String> processor);

        // todo: easier way to constructed processor?
        default Builder processor(ValueOptions options) {
            // todo: remove
            return processor(new ObjectProcessorJsonValueFromString(new JsonFactory(), options).named());
        }

        Builder chunkSize(int chunkSize);

        Builder skipFirstN(int skipFirstN);

        WebsocketTableOptions build();
    }


}
