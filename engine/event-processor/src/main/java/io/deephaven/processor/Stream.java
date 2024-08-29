package io.deephaven.processor;

import io.deephaven.processor.appender.Appender;

import java.util.List;

public interface Stream {
    void ensureRemainingCapacity(long size);

     void advanceAll(); // can only use if row-oriented?

    List<Appender> appenders();
}
