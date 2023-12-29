/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.deephaven.json.Helpers.assertCurrentToken;
import static io.deephaven.json.Helpers.assertNextToken;

/**
 * @deprecated see {@link ObjectProcessorJsonValue}
 */
@Deprecated
public final class ObjectProcessorJsonObject implements ObjectProcessor<byte[]> {

    public static ObjectProcessorJsonObject example() {
        return new ObjectProcessorJsonObject(new JsonFactory(), ObjectOptions.builder()
                .putFields("timestamp", InstantOptions.standard())
                .putFields("age", IntOptions.standard())
                .putFields("height", DoubleOptions.standard())
                .build());
    }

    private final JsonFactory jsonFactory;
    private final ObjectOptions opts;

    public ObjectProcessorJsonObject(JsonFactory jsonFactory, ObjectOptions opts) {
        this.jsonFactory = Objects.requireNonNull(jsonFactory);
        this.opts = Objects.requireNonNull(opts);
    }

    @Override
    public List<Type<?>> outputTypes() {
        return opts.outputTypes().collect(Collectors.toList());
    }

    @Override
    public void processAll(ObjectChunk<? extends byte[], ?> in, List<WritableChunk<?>> out) {
        final ValueProcessor objectProcessor = opts.processor("<root>", out);
        for (int i = 0; i < in.size(); ++i) {
            try (final JsonParser parser = jsonFactory.createParser(in.get(i))) {
                assertNextToken(parser, JsonToken.START_OBJECT);
                objectProcessor.processCurrentValue(parser);
                assertCurrentToken(parser, JsonToken.END_OBJECT);
                assertNextToken(parser, null);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
