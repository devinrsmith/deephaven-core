/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.ObjectOptions;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;

import static io.deephaven.json.jackson.Helpers.assertCurrentToken;
import static io.deephaven.json.jackson.Helpers.assertNextToken;

public final class ObjectProcessorJsonObjectArray implements ObjectProcessor<byte[]> {
    private final JsonFactory jsonFactory;
    private final ObjectOptions opts;

    public ObjectProcessorJsonObjectArray(JsonFactory jsonFactory, ObjectOptions opts) {
        this.jsonFactory = Objects.requireNonNull(jsonFactory);
        this.opts = Objects.requireNonNull(opts);
    }

    @Override
    public int size() {
        return 0;
        // return opts.outputCount();
    }

    @Override
    public List<Type<?>> outputTypes() {
        return null;
        // return opts.outputTypes().collect(Collectors.toList());
    }

    @Override
    public void processAll(ObjectChunk<? extends byte[], ?> in, List<WritableChunk<?>> out) {
        final ValueProcessor objectProcessor = null; // opts.processor("<root>", out);
        for (int i = 0; i < in.size(); ++i) {
            try (final JsonParser parser = jsonFactory.createParser(in.get(i))) {
                assertNextToken(parser, JsonToken.START_ARRAY);
                objectProcessor.processCurrentValue(parser);
                assertCurrentToken(parser, JsonToken.END_ARRAY);
                assertNextToken(parser, null);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
