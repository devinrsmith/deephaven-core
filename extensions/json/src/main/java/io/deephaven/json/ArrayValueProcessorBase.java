/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;

abstract class ArrayValueProcessorBase<P extends ValueProcessor> extends ValueProcessorBase {

    ArrayValueProcessorBase(String contextPrefix, boolean allowNull, boolean allowMissing) {
        super(contextPrefix, allowNull, allowMissing);
    }

    protected abstract P start();

    protected abstract void end(P valueProcessor);

    @Override
    protected final void handleValueArray(JsonParser parser) throws IOException {
        final P elementProcessor = start();
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            elementProcessor.processCurrentValue(parser);
        }
        end(elementProcessor);
    }
}
