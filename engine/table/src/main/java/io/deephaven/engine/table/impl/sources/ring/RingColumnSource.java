package io.deephaven.engine.table.impl.sources.ring;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.BooleanArraySource;
import io.deephaven.engine.table.impl.sources.ByteArraySource;
import io.deephaven.engine.table.impl.sources.CharacterArraySource;
import io.deephaven.engine.table.impl.sources.DateTimeArraySource;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.engine.table.impl.sources.FloatArraySource;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.ShortArraySource;
import io.deephaven.time.DateTime;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Objects;

public final class RingColumnSource<T>
        extends AbstractColumnSource<T>
        implements InMemoryColumnSource {

    public static RingColumnSource<Byte> ofByte(int capacity) {
        return ByteRingChunkSource.columnSource(capacity);
    }

    public static RingColumnSource<Character> ofCharacter(int capacity) {
        return CharacterRingChunkSource.columnSource(capacity);
    }

    public static RingColumnSource<Double> ofDouble(int capacity) {
        return DoubleRingChunkSource.columnSource(capacity);
    }

    public static RingColumnSource<Float> ofFloat(int capacity) {
        return FloatRingChunkSource.columnSource(capacity);
    }

    public static RingColumnSource<Integer> ofInteger(int capacity) {
        return IntegerRingChunkSource.columnSource(capacity);
    }

    public static RingColumnSource<Long> ofLong(int capacity) {
        return LongRingChunkSource.columnSource(capacity);
    }

    public static RingColumnSource<Short> ofShort(int capacity) {
        return ShortRingChunkSource.columnSource(capacity);
    }

    @SuppressWarnings("unchecked")
    public static <T> RingColumnSource<T> of(int capacity, Class<T> dataType, Class<?> componentType) {
        if (dataType == byte.class || dataType == Byte.class) {
            return (RingColumnSource<T>) ofByte(capacity);
        } else if (dataType == char.class || dataType == Character.class) {
            return (RingColumnSource<T>) ofCharacter(capacity);
        } else if (dataType == double.class || dataType == Double.class) {
            return (RingColumnSource<T>) ofDouble(capacity);
        } else if (dataType == float.class || dataType == Float.class) {
            return (RingColumnSource<T>) ofFloat(capacity);
        } else if (dataType == int.class || dataType == Integer.class) {
            return (RingColumnSource<T>) ofInteger(capacity);
        } else if (dataType == long.class || dataType == Long.class) {
            return (RingColumnSource<T>) ofLong(capacity);
        } else if (dataType == short.class || dataType == Short.class) {
            return (RingColumnSource<T>) ofShort(capacity);
        } else if (dataType == boolean.class || dataType == Boolean.class) {
            throw new UnsupportedOperationException("todo");
        } else if (dataType == DateTime.class) {
            throw new UnsupportedOperationException("todo");
        } else {
            throw new UnsupportedOperationException("todo");
        }
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
        dst.copyCurrentToPrevious(fillContext, getContext);
        dst.append(src, fillContext, getContext, 0, 31);

        TrackingWritableRowSet rowSet = RowSetFactory.flat(64).toTracking();

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

    public void copyCurrentToPrevious(FillContext fillContext, GetContext context) {
        // noinspection unchecked,rawtypes
        ((AbstractRingChunkSource) prev).replayFrom(ring, fillContext, context);
    }

    public void append(
            ColumnSource<T> src, FillContext fillContext, GetContext context, long firstKey, long lastKey) {
        ring.append(src, fillContext, context, firstKey, lastKey);
    }

    public void updateTracking(WritableRowSet rowSet) {
        final RowSet currRowSet = ring.rowSet();
        final RowSet prevRowSet = prev.rowSet();
        final WritableRowSet intersect = currRowSet.intersect(prevRowSet);
        rowSet.update(currRowSet.minus(intersect), prevRowSet.minus(intersect));
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
