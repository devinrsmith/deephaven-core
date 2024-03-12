//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.json.jackson.RepeaterProcessor.Context;

import java.io.IOException;
import java.util.Objects;

final class ValueProcessorArrayImpl implements ValueProcessor {

    static void processArray(
            RepeaterProcessor elementProcessor,
            JsonParser parser,
            Runnable processElementCallback) throws IOException {
        Parsing.assertCurrentToken(parser, JsonToken.START_ARRAY);
        final Context context = elementProcessor.start(parser);
        parser.nextToken();
        int ix;
        for (ix = 0; !parser.hasToken(JsonToken.END_ARRAY); ++ix) {
            context.processElement(parser, ix);
            parser.nextToken();
            if (processElementCallback != null) {
                processElementCallback.run();
            }
        }
        context.done(parser, ix);
    }

    static void processArray2(
            ValueProcessor elementProcessor,
            JsonParser parser,
            Runnable processElementCallback) throws IOException {
        Parsing.assertCurrentToken(parser, JsonToken.START_ARRAY);
        parser.nextToken();
        while (!parser.hasToken(JsonToken.END_ARRAY)) {
            elementProcessor.processCurrentValue(parser);
            parser.nextToken();
            if (processElementCallback != null) {
                processElementCallback.run();
            }
        }
    }

    private final RepeaterProcessor elementProcessor;

    ValueProcessorArrayImpl(RepeaterProcessor elementProcessor) {
        this.elementProcessor = Objects.requireNonNull(elementProcessor);
    }

    @Override
    public void processCurrentValue(JsonParser parser) throws IOException {
        if (parser.hasToken(JsonToken.VALUE_NULL)) {
            elementProcessor.processNullRepeater(parser);
            return;
        }
        processArray(elementProcessor, parser, null);
    }

    @Override
    public void processMissing(JsonParser parser) throws IOException {
        elementProcessor.processMissingRepeater(parser);
    }
}
