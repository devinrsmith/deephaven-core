/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

interface ArrayProcessor {

    Context start(JsonParser parser) throws IOException;

    void processNullArray(JsonParser parser) throws IOException;

    void processMissingArray(JsonParser parser) throws IOException;

    interface Context {

        boolean hasElement(JsonParser parser);

        void processElement(JsonParser parser, int index) throws IOException;

        // While a traditional arrays can't have missing elements, when an object is an array, a field may be missing:
        // [ { "foo": 1, "bar": 2 }, {"bar": 3} ]
        void processElementMissing(JsonParser parser, int index) throws IOException;

        void done(JsonParser parser, int length) throws IOException;
    }
}
