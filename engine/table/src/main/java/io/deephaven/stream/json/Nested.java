//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.stream.json;


import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.processor.factory.EventProcessorFactory.EventProcessor;
import io.deephaven.processor.sink.Coordinator;
import io.deephaven.processor.sink.Sink;
import io.deephaven.processor.sink.Stream;
import io.deephaven.processor.sink.appender.IntAppender;
import io.deephaven.processor.sink.appender.LongAppender;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.SingleBlinkCoordinator;
import io.deephaven.stream.StreamToBlinkTableAdapter;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static io.deephaven.stream.json.JsonEventStuff.intValue;
import static io.deephaven.stream.json.JsonEventStuff.startArray;

public class Nested implements EventProcessor<JsonParser> {

    public static Table read() {
        final SingleBlinkCoordinator coord =
                new SingleBlinkCoordinator(List.of(Type.longType(), Type.longType(), Type.intType()));
        final TableDefinition tableDefinition = TableDefinition.of(
                ColumnDefinition.ofLong("OuterIx"),
                ColumnDefinition.ofLong("InnerIx"),
                ColumnDefinition.ofInt("Value"));
        final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(tableDefinition, coord,
                ExecutionContext.getContext().getUpdateGraph(), "test");
        final Thread thread = new Thread(() -> {
            final Nested nested = new Nested(Sink.builder().coordinator(coord).addStreams(coord).build());
            try (final JsonParser parser = new JsonFactory().createParser(new File("/tmp/test.json"))) {
                parser.nextToken();
                coord.writing();
                nested.writeToSink(parser);
                coord.sync();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        thread.start();
        return adapter.table();
    }

    private final Coordinator coordinator;
    private final Stream stream;
    private final LongAppender outerIndex;
    private final LongAppender innerIndex;
    private final IntAppender value;

    private Nested(Sink sink) {
        if (sink.streams().size() != 1) {
            throw new IllegalArgumentException();
        }
        this.coordinator = sink.coordinator();
        this.stream = sink.streams().get(0);
        if (stream.appenders().size() != 3) {
            throw new IllegalArgumentException();
        }
        this.outerIndex = LongAppender.get(stream.appenders().get(0));
        this.innerIndex = LongAppender.get(stream.appenders().get(1));
        this.value = IntAppender.get(stream.appenders().get(2));
    }

    @Override
    public void writeToSink(JsonParser event) {
        try {
            writeToSinkJackson(event);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        // noop
    }

    private void writeToSinkJackson(JsonParser parser) throws IOException {
        startArray(parser);
        for (long outerIx = 0; parser.nextToken() != JsonToken.END_ARRAY; ++outerIx) {
            startArray(parser);
            for (long innerIx = 0; parser.nextToken() != JsonToken.END_ARRAY; ++innerIx) {
                startArray(parser);
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    // no yield, we want to only deliver inner arrays together
                    outerIndex.set(outerIx);
                    innerIndex.set(innerIx);
                    value.set(intValue(parser));
                    stream.advanceAll();
                }
                coordinator.yield();
            }
            // Don't need outer yield given inner yield
            // coordinator.yield();
        }
    }
}
