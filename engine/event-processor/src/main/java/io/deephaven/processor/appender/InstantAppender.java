package io.deephaven.processor.appender;

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
