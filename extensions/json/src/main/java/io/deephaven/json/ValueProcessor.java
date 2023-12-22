/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

interface ValueProcessor {

    static ValueProcessor skip() {
        return SkipProcessor.INSTANCE;
    }

    void processCurrentValue(JsonParser parser) throws IOException;

    void processMissing(JsonParser parser) throws IOException;
}
