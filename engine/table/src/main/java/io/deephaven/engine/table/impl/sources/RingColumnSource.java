package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public final class RingColumnSource<T, ARRAY, RING extends AbstractRingChunkSource<T, ARRAY, RING>>
        extends AbstractColumnSource<T>
        implements InMemoryColumnSource {

    public static RingColumnSource<Byte, byte[], ByteRingChunkSource> ofByte(int n) {
        final ByteRingChunkSource ring = new ByteRingChunkSource(n);
        final ByteRingChunkSource prevRing = new ByteRingChunkSource(n);
        return new RingColumnSource<>(byte.class, ring, prevRing);
    }

    public static RingColumnSource<Integer, int[], IntRingChunkSource> ofInt(int n) {
        final IntRingChunkSource ring = new IntRingChunkSource(n);
        final IntRingChunkSource prevRing = new IntRingChunkSource(n);
        return new RingColumnSource<>(int.class, ring, prevRing);
    }

    private final RING ring;
    private final RING prevRing;

    private RingColumnSource(@NotNull Class<T> type, RING ring, RING prevRing) {
        super(type);
        this.ring = Objects.requireNonNull(ring);
        this.prevRing = Objects.requireNonNull(prevRing);
    }

    public int n() {
        return ring.n();
    }

    public void copyCurrentToPrevious() {
        prevRing.copyFrom(ring);
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        return ring.getChunk(context, rowSequence);
    }

    @Override
    public Chunk<Values> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        return ring.getChunk(context, firstKey, lastKey);
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
        ring.fillChunk(context, destination, rowSequence);
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
        prevRing.fillChunk(context, destination, rowSequence);
    }

    @Override
    public Chunk<? extends Values> getPrevChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        return prevRing.getChunk(context, rowSequence);
    }

    @Override
    public Chunk<? extends Values> getPrevChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        return prevRing.getChunk(context, firstKey, lastKey);
    }

    @Override
    public boolean isImmutable() {
        return false;
    }

    @Override
    public T get(long index) {
        return ring.get(index);
    }

    @Override
    public Boolean getBoolean(long index) {
        return ring.getBoolean(index);
    }

    @Override
    public byte getByte(long index) {
        return ring.getByte(index);
    }

    @Override
    public char getChar(long index) {
        return ring.getChar(index);
    }

    @Override
    public double getDouble(long index) {
        return ring.getDouble(index);
    }

    @Override
    public float getFloat(long index) {
        return ring.getFloat(index);
    }

    @Override
    public int getInt(long index) {
        return ring.getInt(index);
    }

    @Override
    public long getLong(long index) {
        return ring.getLong(index);
    }

    @Override
    public short getShort(long index) {
        return ring.getShort(index);
    }

    @Override
    public T getPrev(long index) {
        return prevRing.get(index);
    }

    @Override
    public Boolean getPrevBoolean(long index) {
        return prevRing.getBoolean(index);
    }

    @Override
    public byte getPrevByte(long index) {
        return prevRing.getByte(index);
    }

    @Override
    public char getPrevChar(long index) {
        return prevRing.getChar(index);
    }

    @Override
    public double getPrevDouble(long index) {
        return prevRing.getDouble(index);
    }

    @Override
    public float getPrevFloat(long index) {
        return prevRing.getFloat(index);
    }

    @Override
    public int getPrevInt(long index) {
        return prevRing.getInt(index);
    }

    @Override
    public long getPrevLong(long index) {
        return prevRing.getLong(index);
    }

    @Override
    public short getPrevShort(long index) {
        return prevRing.getShort(index);
    }
}
