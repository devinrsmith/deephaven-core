/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.json.TimestampOptions.Format;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.Objects;

final class TimestampIntProcessor extends ValueProcessorBase {

    private static int scale(Format format) {
        switch (format) {
            case EPOCH_SECONDS:
                return 1_000_000_000;
            case EPOCH_MILLIS:
                return 1_000_000;
            case EPOCH_MICROS:
                return 1_000;
            case EPOCH_NANOS:
                return 1;
            default:
                throw new IllegalArgumentException("Unexpected format: " + format);
        }
    }

    private final WritableLongChunk<?> chunk;
    private final int scale;

    TimestampIntProcessor(String contextPrefix, boolean allowNull, boolean allowMissing, WritableLongChunk<?> chunk,
            Format format) {
        super(contextPrefix, allowNull, allowMissing);
        this.chunk = Objects.requireNonNull(chunk);
        this.scale = scale(format);
    }

    @Override
    protected void handleValueNumberInt(JsonParser parser) throws IOException {
        final long epochNanos = parser.getLongValue() * scale;
        chunk.add(epochNanos);
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
