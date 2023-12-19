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

public final class ObjectProcessorOptions implements ObjectProcessor<byte[]> {
    private final JsonFactory jsonFactory;
    private final ValueOptions opts;

    public ObjectProcessorOptions(JsonFactory jsonFactory, ValueOptions opts) {
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
                final JsonToken startToken = parser.nextToken();
                final JsonToken endToken = opts.startEndTokens().get(startToken);
                if (endToken == null) {
                    throw new IllegalStateException();
                }
                objectProcessor.processCurrentValue(parser);
                assertCurrentToken(parser, endToken);
                assertNextToken(parser, null);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
