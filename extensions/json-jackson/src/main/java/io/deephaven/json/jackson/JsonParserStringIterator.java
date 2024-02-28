/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

final class JsonParserStringIterator implements Iterator<String> {

    private final JsonParser parser;

    public JsonParserStringIterator(JsonParser parser) {
        this.parser = Objects.requireNonNull(parser);
    }

    @Override
    public boolean hasNext() {
        return parser.hasToken(JsonToken.VALUE_STRING);
    }

    @Override
    public String next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        final String ret;
        try {
            ret = parser.getText();
            parser.nextToken();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return ret;
    }
}
