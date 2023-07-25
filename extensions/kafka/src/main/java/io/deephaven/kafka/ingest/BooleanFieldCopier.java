package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.stream.blink.tf.BooleanFunction;
import io.deephaven.stream.blink.tf.ChunkUtils;

import java.util.Objects;

class BooleanFieldCopier implements FieldCopier {
    public static BooleanFieldCopier of(BooleanFunction<Object> f) {
        return new BooleanFieldCopier(f);
    }

    private final BooleanFunction<Object> f;

    private BooleanFieldCopier(BooleanFunction<Object> f) {
        this.f = Objects.requireNonNull(f);
    }

    @Override
    public void copyField(
            ObjectChunk<Object, Values> inputChunk,
            WritableChunk<Values> publisherChunk,
            int sourceOffset,
            int destOffset,
            int length) {
        ChunkUtils.applyInto(f, inputChunk, sourceOffset, publisherChunk.asWritableBooleanChunk(), destOffset, length);
    }
}
