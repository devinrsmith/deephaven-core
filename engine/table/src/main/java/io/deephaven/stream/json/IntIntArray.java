//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.stream.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.processor.sink.Coordinator;
import io.deephaven.processor.sink.Stream;
import io.deephaven.processor.sink.appender.IntAppender;

import java.io.IOException;
import java.util.Objects;

import static io.deephaven.stream.json.JsonEventStuff.intValue;
import static io.deephaven.stream.json.JsonEventStuff.startArray;

public class IntIntArray implements JsonEventStuff {


    private final Coordinator coordinator;
    private final Stream stream;
    private final IntSmallArray array;

    public IntIntArray(Coordinator coordinator, Stream stream, IntAppender appender) {
        this.coordinator = coordinator;
        this.stream = Objects.requireNonNull(stream);
        this.array = new IntSmallArray(stream, appender);
    }

    @Override
    public void writeToSink(JsonParser parser) throws IOException {
        startArray(parser);
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            coordinator.yield();
            array.writeToSink(parser);
            stream.advanceAll();
        }
    }
}
