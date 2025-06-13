//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;

final class JacksonArrayIterator extends JacksonIterator {

    JacksonArrayIterator(ValueProcessor processor, JsonParser parser, int chunkCapacity) {
        super(processor, parser, chunkCapacity);
        if (!parser.getParsingContext().getParent().inArray()) {
            throw new IllegalArgumentException(String.format("Expected parent to be an array. ptr @ '%s'",
                    parser.getParsingContext().pathAsPointer()));
        }
    }

    @Override
    public boolean hasNext() {
        return parser.currentToken() != JsonToken.END_ARRAY;
    }
}
