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

import static io.deephaven.util.QueryConstants.NULL_SHORT;

final class ShortRingChunkSource extends AbstractRingChunkSource<Short, short[], ShortRingChunkSource> {
    public static RingColumnSource<Short> columnSource(int n) {
        return new RingColumnSource<>(short.class, new ShortRingChunkSource(n), new ShortRingChunkSource(n));
    }

    public ShortRingChunkSource(int n) {
        super(short.class, n);
    }

    @Override
    public ChunkType getChunkType() {
        return ChunkType.Short;
    }

    @Override
    void clear() {
        Arrays.fill(ring, NULL_SHORT);
    }

    @Override
    void fillKey(@NotNull WritableChunk<? super Values> destination, int destOffset, int ix) {
        destination.asWritableShortChunk().set(destOffset, ring[ix]);
    }

    @Override
    Short get(long key) {
        return TypeUtils.box(getShort(key));
    }

    @Override
    short getShort(long key) {
        if (!containsIndex(key)) {
            return NULL_SHORT;
        }
        return ring[keyToRingIndex(key)];
    }
}
