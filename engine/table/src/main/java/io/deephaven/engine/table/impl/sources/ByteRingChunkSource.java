package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_BYTE;

public final class ByteRingChunkSource extends AbstractRingChunkSource<Byte, byte[], ByteRingChunkSource> {
    public ByteRingChunkSource(int n) {
        super(byte.class, n);
    }

    @Override
    public ChunkType getChunkType() {
        return ChunkType.Byte;
    }

    @Override
    void fillKey(@NotNull WritableChunk<? super Values> destination, int destOffset, int ix) {
        destination.asWritableByteChunk().set(destOffset, ring[ix]);
    }

    @Override
    Byte get(long key) {
        return TypeUtils.box(getByte(key));
    }

    @Override
    int getInt(long key) {
        if (!containsIndex(key)) {
            return NULL_BYTE;
        }
        return getByteUnsafe(key);
    }

    public int getByteUnsafe(long key) {
        return ring[keyToRingIndex(key)];
    }
}
