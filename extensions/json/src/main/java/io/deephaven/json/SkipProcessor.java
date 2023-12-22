/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

enum SkipProcessor implements ValueProcessor {
    INSTANCE;

    @Override
    public void processCurrentValue(JsonParser parser) throws IOException {
        parser.skipChildren();
    }

    @Override
    public void processMissing(JsonParser parser) throws IOException {

    }
}
