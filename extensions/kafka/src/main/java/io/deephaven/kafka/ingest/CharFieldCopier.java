package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.stream.blink.tf.CharFunction;
import io.deephaven.stream.blink.tf.ObjectFunction;
import io.deephaven.util.type.TypeUtils;

import java.util.Objects;

class CharFieldCopier implements FieldCopier {
    public static CharFieldCopier of(CharFunction<Object> f) {
        return new CharFieldCopier(f);
    }

    public static CharFieldCopier of(ObjectFunction<Object, Character> f) {
        return of(f.mapChar(TypeUtils::unbox));
    }

    private final CharFunction<Object> f;

    private CharFieldCopier(CharFunction<Object> f) {
        this.f = Objects.requireNonNull(f);
    }

    @Override
    public void copyField(
            ObjectChunk<Object, Values> inputChunk,
            WritableChunk<Values> publisherChunk,
            int sourceOffset,
            int destOffset,
            int length) {
        final WritableCharChunk<Values> output = publisherChunk.asWritableCharChunk();
        for (int ii = 0; ii < length; ++ii) {
            output.set(ii + destOffset, f.applyAsChar(inputChunk.get(ii + sourceOffset)));
        }
    }
}
