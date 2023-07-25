package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.stream.blink.tf.ByteFunction;
import io.deephaven.stream.blink.tf.ObjectFunction;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.type.TypeUtils;

import java.util.Objects;

class ByteFieldCopier implements FieldCopier {
    public static ByteFieldCopier of(ByteFunction<Object> f) {
        return new ByteFieldCopier(f);
    }

    public static ByteFieldCopier of(ObjectFunction<Object, Byte> f) {
        return of(f.mapByte(TypeUtils::unbox));
    }

    public static ByteFieldCopier ofBoolean(ObjectFunction<Object, Boolean> f) {
        return of(f.mapByte(BooleanUtils::booleanAsByte));
    }

    private final ByteFunction<Object> f;

    private ByteFieldCopier(ByteFunction<Object> f) {
        this.f = Objects.requireNonNull(f);
    }

    @Override
    public void copyField(
            ObjectChunk<Object, Values> inputChunk,
            WritableChunk<Values> publisherChunk,
            int sourceOffset,
            int destOffset,
            int length) {
        final WritableByteChunk<Values> output = publisherChunk.asWritableByteChunk();
        for (int ii = 0; ii < length; ++ii) {
            output.set(ii + destOffset, f.applyAsByte(inputChunk.get(ii + sourceOffset)));
        }
    }
}
