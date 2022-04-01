package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_INT;

public final class IntRingChunkSource extends AbstractRingChunkSource<Integer, int[]> {
    public IntRingChunkSource(int n) {
        super(int.class, n);
    }

    @Override
    public ChunkType getChunkType() {
        return ChunkType.Int;
    }

    @Override
    void fillKey(@NotNull WritableChunk<? super Values> destination, int destOffset, int ix) {
        destination.asWritableIntChunk().set(destOffset, ring[ix]);
    }

    @Override
    Integer get(long key) {
        return TypeUtils.box(getInt(key));
    }

    @Override
    int getInt(long key) {
        if (!containsIndex(key)) {
            return NULL_INT;
        }
        return getIntUnsafe(key);
    }

    public int getIntUnsafe(long index) {
        final int bufferIx = (int) (index % n);
        return ring[bufferIx];
    }
}
