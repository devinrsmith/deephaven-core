//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.sink.appender;

import java.math.RoundingMode;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

public interface InstantAppender extends ObjectAppender<Instant> {

    static InstantAppender get(Appender appender) {
        return (InstantAppender) appender;
    }

    static void append(InstantAppender appender, Instant value) {
        ObjectAppender.append(appender, value);
    }

    static void appendNull(InstantAppender appender) {
        ObjectAppender.appendNull(appender);
    }

    LongAppender asLongEpochAppender(TimeUnit unit);

    DoubleAppender asDoubleEpochAppender(TimeUnit unit, RoundingMode roundingMode);

    ObjectAppender<String> asStringEpochConsumer(TimeUnit unit, RoundingMode roundingMode);
}
