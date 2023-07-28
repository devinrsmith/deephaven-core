package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.stream.blink.tf.ChunkUtils;
import io.deephaven.stream.blink.tf.ObjectFunction;

import java.util.Objects;

class ObjectFieldCopier implements FieldCopier {
    public static ObjectFieldCopier of(ObjectFunction<Object, ?> f) {
        return new ObjectFieldCopier(f);
    }

    private final ObjectFunction<Object, ?> f;

    private ObjectFieldCopier(ObjectFunction<Object, ?> f) {
        this.f = Objects.requireNonNull(f);
    }

    @Override
    public void copyField(
            ObjectChunk<Object, Values> inputChunk,
            WritableChunk<Values> publisherChunk,
            int sourceOffset,
            int destOffset,
            int length) {
        ChunkUtils.applyInto(f, inputChunk, sourceOffset, publisherChunk.asWritableObjectChunk(), destOffset, length);
    }
}