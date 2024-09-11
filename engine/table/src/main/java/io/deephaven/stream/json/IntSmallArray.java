//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.stream.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.processor.sink.Stream;
import io.deephaven.processor.sink.appender.IntAppender;

import java.io.IOException;
import java.util.Objects;

import static io.deephaven.stream.json.JsonEventStuff.intValue;
import static io.deephaven.stream.json.JsonEventStuff.startArray;

public class IntSmallArray implements JsonEventStuff {

    // "small" signifies that we don't need to yield.

    private final Stream stream;
    private final IntAppender appender;

    public IntSmallArray(Stream stream, IntAppender appender) {
        this.stream = Objects.requireNonNull(stream);
        this.appender = Objects.requireNonNull(appender);
    }

    @Override
    public void writeToSink(JsonParser parser) throws IOException {
        startArray(parser);
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            appender.set(intValue(parser));
            stream.advanceAll();
        }
    }
}
