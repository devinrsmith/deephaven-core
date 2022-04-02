package io.deephaven.engine.table.impl.sources.ring;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Objects;

public final class RingColumnSource<T>
        extends AbstractColumnSource<T>
        implements InMemoryColumnSource {

    public static RingColumnSource<Byte> ofByte(int n) {
        return ByteRingChunkSource.columnSource(n);
    }

    public static RingColumnSource<Character> ofCharacter(int n) {
        return CharacterRingChunkSource.columnSource(n);
    }

    public static RingColumnSource<Double> ofDouble(int n) {
        return DoubleRingChunkSource.columnSource(n);
    }

    public static RingColumnSource<Float> ofFloat(int n) {
        return FloatRingChunkSource.columnSource(n);
    }

    public static RingColumnSource<Integer> ofInteger(int n) {
        return IntegerRingChunkSource.columnSource(n);
    }

    public static RingColumnSource<Long> ofLong(int n) {
        return LongRingChunkSource.columnSource(n);
    }

    public static RingColumnSource<Short> ofShort(int n) {
        return ShortRingChunkSource.columnSource(n);
    }

    public static Table example() {
        LongArraySource src = new LongArraySource();
        src.ensureCapacity(32);
        for (long i = 0; i < 32; ++i) {
            src.set(i, i);
        }

        FillContext fillContext = src.makeFillContext(4096);
        GetContext getContext = src.makeGetContext(4096);

        RingColumnSource<Long> dst = RingColumnSource.ofLong(32768);
        dst.append(src, fillContext, getContext, 0, 31);

        TrackingWritableRowSet rowSet = RowSetFactory.flat(32).toTracking();

        return new QueryTable(rowSet, Collections.singletonMap("R", dst));
    }

    private final AbstractRingChunkSource<T, ?, ?> ring;
    private final AbstractRingChunkSource<T, ?, ?> prev;

    <ARRAY, RING extends AbstractRingChunkSource<T, ARRAY, RING>> RingColumnSource(
            @NotNull Class<T> type,
            RING ring,
            RING prev) {
        super(type);
        this.ring = Objects.requireNonNull(ring);
        this.prev = Objects.requireNonNull(prev);
    }

    public int capacity() {
        return ring.capacity();
    }

    public void copyCurrentToPrevious() {
        // noinspection unchecked,rawtypes
        ((AbstractRingChunkSource) prev).replayFrom(ring);
    }

    public void append(
            ColumnSource<T> src, FillContext fillContext, GetContext context, long firstKey, long lastKey) {
        ring.append(src, fillContext, context, firstKey, lastKey);
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
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        ring.fillChunk(context, destination, rowSequence);
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        prev.fillChunk(context, destination, rowSequence);
    }

    @Override
    public Chunk<? extends Values> getPrevChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        return prev.getChunk(context, rowSequence);
    }

    @Override
    public Chunk<? extends Values> getPrevChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        return prev.getChunk(context, firstKey, lastKey);
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
        return prev.get(index);
    }

    @Override
    public Boolean getPrevBoolean(long index) {
        return prev.getBoolean(index);
    }

    @Override
    public byte getPrevByte(long index) {
        return prev.getByte(index);
    }

    @Override
    public char getPrevChar(long index) {
        return prev.getChar(index);
    }

    @Override
    public double getPrevDouble(long index) {
        return prev.getDouble(index);
    }

    @Override
    public float getPrevFloat(long index) {
        return prev.getFloat(index);
    }

    @Override
    public int getPrevInt(long index) {
        return prev.getInt(index);
    }

    @Override
    public long getPrevLong(long index) {
        return prev.getLong(index);
    }

    @Override
    public short getPrevShort(long index) {
        return prev.getShort(index);
    }
}
