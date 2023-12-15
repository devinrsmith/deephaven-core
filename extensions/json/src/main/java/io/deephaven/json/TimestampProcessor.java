/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.nio.CharBuffer;
import java.time.Instant;
import java.util.Objects;

final class TimestampProcessor extends ValueProcessorBase {

    private final WritableLongChunk<?> chunk;

    TimestampProcessor(String contextPrefix, boolean allowNull, boolean allowMissing, WritableLongChunk<?> chunk) {
        super(contextPrefix, allowNull, allowMissing);
        this.chunk = Objects.requireNonNull(chunk);
    }

    @Override
    protected void handleValueString(JsonParser parser) throws IOException {
        final char[] chars = parser.getTextCharacters();
        final int offset = parser.getTextOffset();
        final int len = parser.getTextLength();
        // todo: use DateTimeUtils w/ CharSequence?
        final Instant instant = Instant.parse(CharBuffer.wrap(chars, offset, len));
        chunk.add(DateTimeUtils.epochNanos(instant));
    }

    @Override
    public void handleNull() {
        chunk.add(QueryConstants.NULL_LONG);
    }

    @Override
    public void handleMissing() {
        chunk.add(QueryConstants.NULL_LONG);
    }
}
