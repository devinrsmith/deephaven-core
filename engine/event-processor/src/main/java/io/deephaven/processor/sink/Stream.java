//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.sink;

import io.deephaven.processor.sink.appender.Appender;

import java.util.List;
import java.util.Map;

// this is somewhat equivalent to StreamConsumer

public interface Stream {

    static Appender get(Stream stream, Key<?> key) {
        final Appender appender = getIfPresent(stream, key);
        if (appender == null) {
            throw new IllegalStateException("Key not present: " + key);
        }
        return appender;
    }

    static Appender getIfPresent(Stream stream, Key<?> key) {
        return stream.appendersMap().get(key);
    }

    // the chunk equivalent to this is sort of StreamConsumer

    void ensureRemainingCapacity(long size);

    void advanceAll(); // can only use if row-oriented?

    List<? extends Appender> appenders();

    // this is somewhat analogous

    Map<Key<?>, ? extends Appender> appendersMap();

    // if using chunks:

//    List<Key<?>> keys();
//
//    StreamConsumer consumer();
}
