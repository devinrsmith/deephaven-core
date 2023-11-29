/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;

abstract class ArrayValueProcessor<T extends ValueProcessor> extends ValueProcessorBase {

    ArrayValueProcessor(String contextPrefix, boolean allowNull, boolean allowMissing) {
        super(contextPrefix, allowNull, allowMissing);
    }

    protected abstract T start();

    protected abstract void end(T valueProcessor);

    @Override
    protected void handleValueArray(JsonParser parser) throws IOException {
        final T elementProcessor = start();
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            elementProcessor.processCurrentValue(parser);
        }
        end(elementProcessor);
    }
}
