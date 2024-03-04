/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.util.Objects;

final class ArrayValueProcessor implements ValueProcessor {

    private final ValueProcessor elementProcessor;

    ArrayValueProcessor(ValueProcessor elementProcessor) {
        this.elementProcessor = Objects.requireNonNull(elementProcessor);
    }

    @Override
    public void processCurrentValue(JsonParser parser) throws IOException {
        Helpers.assertCurrentToken(parser, JsonToken.START_ARRAY);
        parser.nextToken();
        do {
            elementProcessor.processCurrentValue(parser);
            parser.nextToken();
        } while (!parser.hasToken(JsonToken.END_ARRAY));
    }

    @Override
    public void processMissing(JsonParser parser) throws IOException {
        elementProcessor.processMissing(parser);
    }
}
