package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.stream.blink.tf.FloatFunction;
import io.deephaven.stream.blink.tf.ObjectFunction;
import io.deephaven.util.type.TypeUtils;

import java.util.Objects;

class FloatFieldCopier implements FieldCopier {
    public static FloatFieldCopier of(FloatFunction<Object> f) {
        return new FloatFieldCopier(f);
    }

    public static FloatFieldCopier of(ObjectFunction<Object, Float> f) {
        return of(f.mapFloat(TypeUtils::unbox));
    }

    private final FloatFunction<Object> f;

    private FloatFieldCopier(FloatFunction<Object> f) {
        this.f = Objects.requireNonNull(f);
    }

    @Override
    public void copyField(
            ObjectChunk<Object, Values> inputChunk,
            WritableChunk<Values> publisherChunk,
            int sourceOffset,
            int destOffset,
            int length) {
        final WritableFloatChunk<Values> output = publisherChunk.asWritableFloatChunk();
        for (int ii = 0; ii < length; ++ii) {
            output.set(ii + destOffset, f.applyAsFloat(inputChunk.get(ii + sourceOffset)));
        }
    }
}
