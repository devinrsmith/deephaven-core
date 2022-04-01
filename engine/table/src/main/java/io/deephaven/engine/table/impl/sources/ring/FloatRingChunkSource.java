/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharacterRingChunkSource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources.ring;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

final class FloatRingChunkSource extends AbstractRingChunkSource<Float, float[], FloatRingChunkSource> {
    public static RingColumnSource<Float> columnSource(int n) {
        return new RingColumnSource<>(float.class, new FloatRingChunkSource(n), new FloatRingChunkSource(n));
    }

    public FloatRingChunkSource(int n) {
        super(float.class, n);
    }

    @Override
    public ChunkType getChunkType() {
        return ChunkType.Float;
    }

    @Override
    void clear() {
        Arrays.fill(ring, NULL_FLOAT);
    }

    @Override
    void fillKey(@NotNull WritableChunk<? super Values> destination, int destOffset, int ix) {
        destination.asWritableFloatChunk().set(destOffset, ring[ix]);
    }

    @Override
    Float get(long key) {
        return TypeUtils.box(getFloat(key));
    }

    @Override
    float getFloat(long key) {
        if (!containsIndex(key)) {
            return NULL_FLOAT;
        }
        return ring[keyToRingIndex(key)];
    }
}
