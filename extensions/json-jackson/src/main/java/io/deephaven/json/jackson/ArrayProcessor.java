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

        // While a traditional arrays can't have missing elements, when an object is an array, a field may be missing:
        // [ { "foo": 1, "bar": 2 }, {"bar": 3} ]
        void processElementMissing(int ix, JsonParser parser) throws IOException;

        void done(JsonParser parser) throws IOException;
    }
}
