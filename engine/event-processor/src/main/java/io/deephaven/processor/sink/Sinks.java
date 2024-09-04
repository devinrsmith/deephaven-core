//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.sink;

import io.deephaven.io.log.LogLevel;
import io.deephaven.processor.factory.EventProcessorSpec;
import io.deephaven.processor.factory.EventProcessorStreamSpec;
import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.stream.Collectors;

public final class Sinks {

    public static Sink strict(Sink sink) {
        return SinkStrict.of(sink);
    }

    public static Sink logging(String prefix, LogLevel level, EventProcessorSpec spec) {
        return logging(prefix, level,
                spec.streams().stream().map(EventProcessorStreamSpec::outputTypes).collect(Collectors.toList()));
    }

    public static Sink logging(String prefix, LogLevel level, List<List<Type<?>>> types) {
        return SinkLogging.of(prefix, level, types);
    }

    // todo: blink table EventSink
}
