package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.stream.blink.tf.BooleanFunction;

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
        final WritableBooleanChunk<Values> output = publisherChunk.asWritableBooleanChunk();
        for (int ii = 0; ii < length; ++ii) {
            output.set(ii + destOffset, f.applyAsBoolean(inputChunk.get(ii + sourceOffset)));
        }
    }
}
