package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.stream.blink.tf.IntFunction;
import io.deephaven.stream.blink.tf.ObjectFunction;
import io.deephaven.util.type.TypeUtils;

import java.util.Objects;

class IntFieldCopier implements FieldCopier {
    public static IntFieldCopier of(IntFunction<Object> f) {
        return new IntFieldCopier(f);
    }

    public static IntFieldCopier of(ObjectFunction<Object, Integer> f) {
        return of(f.mapInt(TypeUtils::unbox));
    }

    private final IntFunction<Object> f;

    private IntFieldCopier(IntFunction<Object> f) {
        this.f = Objects.requireNonNull(f);
    }

    @Override
    public void copyField(
            ObjectChunk<Object, Values> inputChunk,
            WritableChunk<Values> publisherChunk,
            int sourceOffset,
            int destOffset,
            int length) {
        final WritableIntChunk<Values> output = publisherChunk.asWritableIntChunk();
        for (int ii = 0; ii < length; ++ii) {
            output.set(ii + destOffset, f.applyAsInt(inputChunk.get(ii + sourceOffset)));
        }
    }
}
