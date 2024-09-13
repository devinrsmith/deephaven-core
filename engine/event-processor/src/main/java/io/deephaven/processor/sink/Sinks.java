//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.sink;

import io.deephaven.io.log.LogLevel;
import io.deephaven.processor.factory.EventProcessorSpec;
import io.deephaven.processor.factory.EventProcessorStreamSpec;
import io.deephaven.processor.sink.Sink.StreamKey;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class Sinks {

    // note: single threaded only
    public static Sink strict(Sink sink) {
        return SinkStrict.of(sink);
    }

    public static Sink logging(String prefix, LogLevel level, EventProcessorSpec spec) {
        return logging(prefix, level, spec.streams());
    }

    public static Sink logging(String prefix, LogLevel level, List<EventProcessorStreamSpec> specs) {
        return logging(prefix, level, specs.stream()
                .collect(Collectors.toMap(EventProcessorStreamSpec::key, EventProcessorStreamSpec::keys)));
    }

    public static Sink logging(String prefix, LogLevel level, Map<StreamKey, Keys> streamKeys) {
        return SinkLogging.of(prefix, level, streamKeys);
    }

    // todo: blink table EventSink
}
