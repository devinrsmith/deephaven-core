package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.stream.blink.tf.ObjectFunction;

import java.util.Objects;

public class ObjectFieldCopier implements FieldCopier {
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
        final WritableObjectChunk<Object, Values> output = publisherChunk.asWritableObjectChunk();
        for (int ii = 0; ii < length; ++ii) {
            output.set(ii + destOffset, f.apply(inputChunk.get(ii + sourceOffset)));
        }
    }
}
