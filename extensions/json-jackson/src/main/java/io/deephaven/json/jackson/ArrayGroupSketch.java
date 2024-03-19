/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.processor.ObjectProcessorToN;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;

public class ArrayGroupSketch implements ObjectProcessorToN<String> {

    @Override
    public List<Type<?>> outputTypes() {
        // { "prices": [ ... ], "sizes": [ ... ] }
        return List.of(Type.doubleType(), Type.intType());
    }

    @Override
    public void processAll(ObjectChunk<? extends String, ?> in, SinkProvider provider) {
        try {
            for (int i = 0; i < in.size(); ++i) {
                process(in.get(i), provider);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void process(String item, SinkProvider provider) throws IOException {
        try (
                final JsonParser source = JacksonSource.of(JacksonConfiguration.defaultFactory(), item);
                final SinkTx sink = provider.tx()) {
            source.nextToken();
            Parsing.assertCurrentToken(source, JsonToken.START_OBJECT);
            source.nextToken();
            Parsing.assertCurrentToken(source, JsonToken.FIELD_NAME);
            source.nextToken();
            Parsing.assertCurrentToken(source, JsonToken.START_ARRAY);
            source.nextToken();

            {
                final DoubleConsumer prices = sink.appendingDoubleConsumer(0);
                while (!source.hasToken(JsonToken.END_ARRAY)) {
                    prices.accept(source.getDoubleValue());
                }
            }

            source.nextToken();
            Parsing.assertCurrentToken(source, JsonToken.FIELD_NAME);
            source.nextToken();
            Parsing.assertCurrentToken(source, JsonToken.START_ARRAY);
            source.nextToken();

            {
                final IntConsumer sizes = sink.appendingIntConsumer(1);
                while (!source.hasToken(JsonToken.END_ARRAY)) {
                    sizes.accept(source.getIntValue());
                }
            }

            source.nextToken();
            Parsing.assertCurrentToken(source, JsonToken.END_OBJECT);
            source.nextToken();
            Parsing.assertCurrentToken(source, null);

            sink.commit(Integer.MAX_VALUE); // todo
        }
    }
}
