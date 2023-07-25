package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.stream.blink.tf.ChunkUtils;
import io.deephaven.stream.blink.tf.LongFunction;
import io.deephaven.stream.blink.tf.ObjectFunction;
import io.deephaven.util.type.TypeUtils;

import java.util.Objects;

class LongFieldCopier implements FieldCopier {
    public static LongFieldCopier of(LongFunction<Object> f) {
        return new LongFieldCopier(f);
    }

    public static LongFieldCopier of(ObjectFunction<Object, Long> f) {
        return of(f.mapLong(TypeUtils::unbox));
    }

    private final LongFunction<Object> f;

    private LongFieldCopier(LongFunction<Object> f) {
        this.f = Objects.requireNonNull(f);
    }

    @Override
    public void copyField(
            ObjectChunk<Object, Values> inputChunk,
            WritableChunk<Values> publisherChunk,
            int sourceOffset,
            int destOffset,
            int length) {
        ChunkUtils.applyInto(f, inputChunk, sourceOffset, publisherChunk.asWritableLongChunk(), destOffset, length);
    }
}
