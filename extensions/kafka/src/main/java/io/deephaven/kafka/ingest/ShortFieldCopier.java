package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.stream.blink.tf.ChunkUtils;
import io.deephaven.stream.blink.tf.ObjectFunction;
import io.deephaven.stream.blink.tf.ShortFunction;
import io.deephaven.util.type.TypeUtils;

import java.util.Objects;

class ShortFieldCopier implements FieldCopier {
    public static ShortFieldCopier of(ShortFunction<Object> f) {
        return new ShortFieldCopier(f);
    }

    public static ShortFieldCopier of(ObjectFunction<Object, Short> f) {
        return of(f.mapShort(TypeUtils::unbox));
    }

    private final ShortFunction<Object> f;

    private ShortFieldCopier(ShortFunction<Object> f) {
        this.f = Objects.requireNonNull(f);
    }

    @Override
    public void copyField(
            ObjectChunk<Object, Values> inputChunk,
            WritableChunk<Values> publisherChunk,
            int sourceOffset,
            int destOffset,
            int length) {
        ChunkUtils.applyInto(f, inputChunk, sourceOffset, publisherChunk.asWritableShortChunk(), destOffset, length);
    }
}
