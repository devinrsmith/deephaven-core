//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.sink;

import io.deephaven.processor.sink.appender.Appender;

import java.util.List;

public interface Stream {

    // the chunk equivalent to this is sort of StreamConsumer

    void ensureRemainingCapacity(long size);

    void advanceAll(); // can only use if row-oriented?

    List<? extends Appender> appenders();
}
