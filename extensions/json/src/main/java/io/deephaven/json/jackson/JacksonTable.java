/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.table.Table;
import io.deephaven.json.JsonTableOptions;
import io.deephaven.json.ObjectOptions;
import io.deephaven.json.Source;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.stream.StreamConsumer;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public final class JacksonTable {

    public static Table execute(JsonTableOptions options) {
        return execute(JacksonConfiguration.defaultFactory(), options);
    }

    public static Table execute(JsonFactory factory, JsonTableOptions options) throws IOException {
        try (final JsonParser parser = JacksonSource.of(factory, options.source())) {

        }
    }

    public static void executeNewlineDelimited(JsonFactory factory, Source source, ObjectOptions options, StreamConsumer consumer) throws IOException {
        /**
         *         void processAllImpl(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) throws IOException {
         *             final ValueProcessor valueProcessor = processor("<root>", out);
         *             for (int i = 0; i < in.size(); ++i) {
         *                 try (final JsonParser parser = createParser(factory, in.get(i))) {
         *                     ValueProcessor.processFullJson(valueProcessor, parser);
         *                 }
         *             }
         *         }
         */

        new ObjectMixin(options, factory)

        // TODO: need richer interface w/ unknown size to begin with
        // todo: blink table vs static
        final ValueProcessor processor = null;
        try (final JsonParser parser = JacksonSource.of(factory, source)) {



            for (int i = 0; i < 1024 && parser.nextToken() == JsonToken.START_OBJECT; ++i) {
                processor.processCurrentValue(parser);
            }
            consumer.accept();
        }
    }

    private static List<WritableChunk<?>> newProcessorChunks(ObjectProcessor<?> processor, int chunkSize) {
        return processor
                .outputTypes()
                .stream()
                .map(ObjectProcessor::chunkType)
                .map(chunkType -> chunkType.makeWritableChunk(chunkSize))
                .peek(wc -> wc.setSize(0))
                .collect(Collectors.toList());
    }
}
