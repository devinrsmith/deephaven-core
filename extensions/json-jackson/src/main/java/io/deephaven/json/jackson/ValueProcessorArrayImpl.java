/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.json.jackson.ArrayProcessor.Context;

import java.io.IOException;
import java.util.Objects;

final class ValueProcessorArrayImpl implements ValueProcessor {

    private final ArrayProcessor arrayProcessor;

    ValueProcessorArrayImpl(ArrayProcessor arrayProcessor) {
        this.arrayProcessor = Objects.requireNonNull(arrayProcessor);
    }

    @Override
    public void processCurrentValue(JsonParser parser) throws IOException {
        if (parser.hasToken(JsonToken.VALUE_NULL)) {
            arrayProcessor.processNull(parser);
            return;
        }
        final Context context = arrayProcessor.start(parser);
        for (int ix = 0; context.hasElement(parser); ++ix) {
            context.processElement(ix, parser);
            parser.nextToken();
        }
        context.done(parser);
    }

    @Override
    public void processMissing(JsonParser parser) throws IOException {
        arrayProcessor.processMissing(parser);
    }
}
