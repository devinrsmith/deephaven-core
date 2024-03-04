/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

interface ArrayProcessor {

    Context start(JsonParser parser) throws IOException;

    void processNull(JsonParser parser) throws IOException;

    void processMissing(JsonParser parser) throws IOException;

    interface Context {

        boolean hasElement(JsonParser parser);

        void processElement(int ix, JsonParser parser) throws IOException;

        void done(JsonParser parser) throws IOException;
    }
}
