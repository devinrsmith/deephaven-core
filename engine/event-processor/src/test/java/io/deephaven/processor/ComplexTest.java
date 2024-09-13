//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor;

import io.deephaven.io.log.LogLevel;
import io.deephaven.processor.factory.EventProcessorFactory;
import io.deephaven.processor.factory.EventProcessorFactory.EventProcessor;
import io.deephaven.processor.sink.Sink;
import io.deephaven.processor.sink.Sinks;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

public class ComplexTest {
    @Test
    void single() throws URISyntaxException, IOException {
        final byte[] data = Files.readAllBytes(Path.of(ComplexTest.class.getResource("/complex-1.json").toURI()));
        final EventProcessorFactory<byte[]> factory = ComplexExample.single();
        final Sink sink =
                Sinks.strict(Sinks.logging(ComplexTest.class.getSimpleName(), LogLevel.INFO, factory.specs()));
        try (final EventProcessor<byte[]> processor = factory.create(sink)) {
            sink.coordinator().writing();
            processor.writeToSink(data);
            sink.coordinator().sync();
        }
    }

    @Test
    void lines() throws URISyntaxException, IOException {
        final File data = Path.of(ComplexTest.class.getResource("/complex-1.lines.json").toURI()).toFile();
        final EventProcessorFactory<File> factory = ComplexExample.jsonLines();
        final Sink sink =
                Sinks.strict(Sinks.logging(ComplexTest.class.getSimpleName(), LogLevel.INFO, factory.specs()));
        try (final EventProcessor<File> processor = factory.create(sink)) {
            sink.coordinator().writing();
            processor.writeToSink(data);
            sink.coordinator().sync();
        }
    }
}
