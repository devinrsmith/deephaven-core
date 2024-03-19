//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.processor.ObjectProcessorToN;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

public class ArrayGroupSketch2 implements ObjectProcessorToN<String> {

    @Override
    public List<Type<?>> outputTypes() {
        // [ { "price": 1.1, "size": 1 }, ... ]
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
            final PositionDoubleConsumer prices = sink.dc(0);
            final PositionIntConsumer sizes = sink.ic(1);
            source.nextToken();
            Parsing.assertCurrentToken(source, JsonToken.START_ARRAY);
            int ix;
            for (ix = 0; !source.hasToken(JsonToken.END_ARRAY); ++ix) {
                Parsing.assertCurrentToken(source, JsonToken.START_OBJECT);
                source.nextToken();
                Parsing.assertCurrentToken(source, JsonToken.FIELD_NAME);
                source.nextToken();
                // TODO assert int or float
                prices.set(ix, source.getValueAsDouble());
                source.nextToken();
                Parsing.assertCurrentToken(source, JsonToken.FIELD_NAME);
                source.nextToken();
                Parsing.assertCurrentToken(source, JsonToken.VALUE_NUMBER_INT);
                sizes.set(ix, source.getValueAsInt());
                source.nextToken();
                Parsing.assertCurrentToken(source, JsonToken.END_OBJECT);
                source.nextToken();
            }
            sink.commit(ix);
        }
    }
}
