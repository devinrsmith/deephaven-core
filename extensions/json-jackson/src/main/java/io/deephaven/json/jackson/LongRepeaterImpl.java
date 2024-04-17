//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.sized.SizedLongChunk;
import io.deephaven.json.jackson.LongValueProcessor.ToLong;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

final class LongRepeaterImpl extends RepeaterProcessorBase<long[]> {

    private final SizedLongChunk<?> chunk = new SizedLongChunk<>(0);

    private final ToLong toLong;

    public LongRepeaterImpl(ToLong toLong, boolean allowMissing, boolean allowNull) {
        super(allowMissing, allowNull, null, null);
        this.toLong = Objects.requireNonNull(toLong);
    }

    @Override
    public void processElement(JsonParser parser) throws IOException {
        final int newSize = index + 1;
        final WritableLongChunk<?> chunk = this.chunk.ensureCapacityPreserve(newSize);
        chunk.set(index, toLong.parseValue(parser));
        chunk.setSize(newSize);
    }

    @Override
    public void processElementMissing(JsonParser parser) throws IOException {
        final int newSize = index + 1;
        final WritableLongChunk<?> chunk = this.chunk.ensureCapacityPreserve(newSize);
        chunk.set(index, toLong.parseMissing(parser));
        chunk.setSize(newSize);
    }

    @Override
    public long[] doneImpl(JsonParser parser, int length) {
        final WritableLongChunk<?> chunk = this.chunk.get();
        return Arrays.copyOfRange(chunk.array(), chunk.arrayOffset(), chunk.arrayOffset() + length);
    }
}
