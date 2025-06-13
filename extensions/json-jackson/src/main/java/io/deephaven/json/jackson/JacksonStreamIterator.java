//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;

final class JacksonStreamIterator extends JacksonIterator {

    JacksonStreamIterator(ValueProcessor processor, JsonParser parser, int bufferSize) {
        super(processor, parser, bufferSize);
        // TODO: does this hold if we are parsing INTs?
        if (!parser.getParsingContext().getParent().inRoot()) {
            throw new IllegalArgumentException(String.format("Expected parent to be at the root. ptr @ '%s'",
                    parser.getParsingContext().pathAsPointer()));
        }
    }

    @Override
    public boolean hasNext() {
        return parser.hasCurrentToken();
    }
}
