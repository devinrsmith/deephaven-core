//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;

final class JacksonArrayIterator extends JacksonIterator {

    JacksonArrayIterator(ValueProcessor processor, JsonParser parser, int bufferSize) throws IOException {
        super(processor, parser, bufferSize);
        if (parser.isExpectedStartArrayToken()) {
            parser.nextToken();
        } else {
            if (!parser.getParsingContext().inArray()) {
                throw new IllegalArgumentException();
            }
        }
    }

    @Override
    public boolean hasNext() {
        return parser.currentToken() != JsonToken.END_ARRAY;
    }
}
