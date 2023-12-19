/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.nio.CharBuffer;
import java.text.ParsePosition;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Objects;

final class TimestampProcessor extends ValueProcessorBase {

    private final WritableLongChunk<?> chunk;
    private final DateTimeFormatter formatter;

    TimestampProcessor(String contextPrefix, boolean allowNull, boolean allowMissing, WritableLongChunk<?> chunk,
            DateTimeFormatter formatter) {
        super(contextPrefix, allowNull, allowMissing);
        this.chunk = Objects.requireNonNull(chunk);
        this.formatter = Objects.requireNonNull(formatter);
    }

    @Override
    protected void handleValueString(JsonParser parser) throws IOException {
        chunk.add(parseToEpochNanos(parser));
    }

    private long parseToEpochNanos(JsonParser parser) throws IOException {
        final TemporalAccessor parsed = formatter.parse(textAsCharSequence(parser));
        final long epochSeconds = parsed.getLong(ChronoField.INSTANT_SECONDS);
        final int nanoOfSecond = parsed.get(ChronoField.NANO_OF_SECOND);
        // todo: overflow
        // io.deephaven.time.DateTimeUtils.safeComputeNanos
        return epochSeconds * 1_000_000_000L + nanoOfSecond;
    }

    private static CharSequence textAsCharSequence(JsonParser parser) throws IOException {
        return parser.hasTextCharacters()
                ? CharBuffer.wrap(parser.getTextCharacters(), parser.getTextOffset(), parser.getTextLength())
                : parser.getText();
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
