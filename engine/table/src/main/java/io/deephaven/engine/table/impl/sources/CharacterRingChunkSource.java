package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

public final class CharacterRingChunkSource extends AbstractRingChunkSource<Character, char[], CharacterRingChunkSource> {
    public CharacterRingChunkSource(int n) {
        super(char.class, n);
    }

    @Override
    public ChunkType getChunkType() {
        return ChunkType.Char;
    }

    @Override
    void fillKey(@NotNull WritableChunk<? super Values> destination, int destOffset, int ix) {
        destination.asWritableCharChunk().set(destOffset, ring[ix]);
    }

    @Override
    Character get(long key) {
        return TypeUtils.box(getChar(key));
    }

    @Override
    char getChar(long key) {
        if (!containsIndex(key)) {
            return NULL_CHAR;
        }
        return getCharUnsafe(key);
    }

    public char getCharUnsafe(long key) {
        return ring[keyToRingIndex(key)];
    }
}
