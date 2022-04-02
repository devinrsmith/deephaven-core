package io.deephaven.engine.table.impl.sources.ring;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.DefaultChunkSource;
import io.deephaven.engine.table.impl.DefaultGetContext;
import io.deephaven.util.datastructures.LongRangeConsumer;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;
import java.util.Objects;
import java.util.function.LongConsumer;

import static io.deephaven.engine.table.impl.AbstractColumnSource.USE_RANGES_AVERAGE_RUN_LENGTH;

abstract class AbstractRingChunkSource<T, ARRAY, SELF extends AbstractRingChunkSource<T, ARRAY, SELF>>
        implements DefaultChunkSource<Values> {

    // todo: should this be a (writable)chunk?
    protected final ARRAY ring;
    protected final int capacity;
    long nextKey;

    public AbstractRingChunkSource(@NotNull Class<T> componentType, int capacity) {
        this.capacity = capacity;
        // noinspection unchecked
        ring = (ARRAY) Array.newInstance(componentType, capacity);
    }

    public final int capacity() {
        return capacity;
    }

    public final int size() {
        return capacity <= nextKey ? capacity : (int) nextKey;
    }

    public final boolean isEmpty() {
        return nextKey == 0;
    }

    public final boolean containsIndex(long key) {
        return key >= 0 && key >= (nextKey - capacity) && key < nextKey;
    }

    public final OrderedLongSet indices() {
        return isEmpty() ? OrderedLongSet.EMPTY : SingleRange.make(nextKey - size(), nextKey - 1);
    }

    // todo: if we can get efficient last N, we can implement this w/ RowSequence
    public final void append(
            ColumnSource<T> src, FillContext fillContext, GetContext context, long firstKey, long lastKey) {
        if (firstKey > lastKey) {
            throw new IllegalArgumentException("Need at least one element to append");
        }
        // todo: should we have our own get context?
        final long logicalSize = lastKey - firstKey + 1;
        final long keyStart;
        final long modifiedFirstKey;
        final int copyLen;
        if (logicalSize <= capacity) {
            keyStart = nextKey;
            modifiedFirstKey = firstKey;
            copyLen = (int) logicalSize;
        } else {
            // If the logical amount of data to copy is greater than the capacity, we only need to copy the last
            // capacity elements from the source.
            final long extraOffset = logicalSize - capacity;
            keyStart = nextKey + extraOffset;
            modifiedFirstKey = firstKey + extraOffset;
            copyLen = capacity;
        }
        // [0, capacity)
        final int copy1Start = keyToRingIndex(keyStart);
        // (0, capacity]
        final int copy1Max = capacity - copy1Start;
        // copy1Len + copy2Len = copyLen
        final int copy1Len = Math.min(copy1Max, copyLen);
        final int copy2Len = copyLen - copy1Len;

        final long modifiedSecondKey = modifiedFirstKey + copy1Len;

        // todo: why does the factory not use SingleRangeRowSequence
        {
            final RowSequence seq1 = RowSequenceFactory.forRange(modifiedFirstKey, modifiedSecondKey - 1);
            final WritableChunk<Values> chunk = DefaultGetContext
                    .getResettableChunk(context)
                    .resetFromArray(ring, copy1Start, copy1Len);
            // todo: should there be a specialized fillChunk w/ range?
            src.fillChunk(fillContext, chunk, seq1);
        }
        if (copy2Len != 0) {
            final RowSequence seq2 = RowSequenceFactory.forRange(modifiedSecondKey, lastKey);
            final WritableChunk<Values> chunk = DefaultGetContext
                    .getResettableChunk(context)
                    .resetFromArray(ring, 0, copy2Len);
            src.fillChunk(fillContext, chunk, seq2);
        }

        // {
        // final Chunk<? extends Values> chunk1 = src.getChunk(context, modifiedFirstKey, modifiedSecondKey - 1);
        // chunk1.copyToArray(0, ring, copy1Start, copy1Len);
        //
        // final Chunk<? extends Values> chunk2 = src.getChunk(context, modifiedSecondKey, lastKey);
        // chunk2.copyToArray(0, ring, 0, copy2Len);
        // }

        nextKey = keyStart + copyLen;
    }

    @Override
    public final Chunk<Values> getChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        if (rowSequence.isContiguous()) {
            return getChunk(context, rowSequence.firstRowKey(), rowSequence.lastRowKey());
        } else {
            final WritableChunk<Values> chunk = DefaultGetContext.getWritableChunk(context);
            fillChunk(DefaultGetContext.getFillContext(context), chunk, rowSequence);
            return chunk;
        }
    }

    @Override
    public final Chunk<Values> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        final int firstIx = keyToRingIndex(firstKey);
        final int lastIx = keyToRingIndex(lastKey);
        if (firstIx <= lastIx) {
            // Easy case, simple view.
            // More efficient than DefaultGetContext.resetChunkFromArray.
            return DefaultGetContext
                    .getResettableChunk(context)
                    .resetFromArray(ring, firstIx, (lastIx - firstIx) + 1);
        } else {
            // Would be awesome if we could have a view of two wrapped DoubleChunks
            // final DoubleChunk<Any> c1 = DoubleChunk.chunkWrap(buffer, firstIx, buffer.length - firstIx);
            // final DoubleChunk<Any> c2 = DoubleChunk.chunkWrap(buffer, 0, lastIx + 1);
            // return view(c1, c2);
            final WritableChunk<Values> chunk = DefaultGetContext.getWritableChunk(context);
            final int len = fillChunkByCopyFromArray(chunk, 0, firstIx, lastIx);
            chunk.setSize(len);
            return chunk;
        }
    }

    @Override
    public final void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        if (rowSequence.getAverageRunLengthEstimate() < USE_RANGES_AVERAGE_RUN_LENGTH) {
            final KeyFiller filler = new KeyFiller(destination);
            rowSequence.forAllRowKeys(filler);
            destination.setSize(filler.destOffset);
        } else {
            final RangeFiller filler = new RangeFiller(destination);
            rowSequence.forAllRowKeyRanges(filler);
            destination.setSize(filler.destOffset);
        }
    }

    private final class KeyFiller implements LongConsumer {
        private final WritableChunk<? super Values> destination;
        private int destOffset;

        public KeyFiller(WritableChunk<? super Values> destination) {
            this.destination = Objects.requireNonNull(destination);
        }

        @Override
        public void accept(long key) {
            final int ix = keyToRingIndex(key);
            fillKey(destination, destOffset, ix);
            ++destOffset;
        }
    }

    private final class RangeFiller implements LongRangeConsumer {
        private final WritableChunk<? super Values> destination;
        private int destOffset;

        public RangeFiller(WritableChunk<? super Values> destination) {
            this.destination = Objects.requireNonNull(destination);
        }

        @Override
        public void accept(long firstKey, long lastKey) {
            final int firstIx = keyToRingIndex(firstKey);
            final int lastIx = keyToRingIndex(lastKey);
            final int len = fillChunkByCopyFromArray(destination, destOffset, firstIx, lastIx);
            destOffset += len;
        }
    }

    private int fillChunkByCopyFromArray(@NotNull WritableChunk<? super Values> destination, int destOffset,
            int firstIx, int lastIx) {
        if (firstIx <= lastIx) {
            final int len = (lastIx - firstIx) + 1;
            destination.copyFromArray(ring, firstIx, destOffset, len);
            return len;
        } else {
            final int fill1Length = capacity - firstIx;
            final int fill2Length = lastIx + 1;
            destination.copyFromArray(ring, firstIx, destOffset, fill1Length);
            destination.copyFromArray(ring, 0, destOffset + fill1Length, fill2Length);
            return fill1Length + fill2Length;
        }
    }

    final int keyToRingIndex(long key) {
        return (int) (key % capacity);
    }

    final void replayFrom(SELF other) {
        // final long distance = nextIx - other.nextIx;
        // if (distance < n / 2) {
        // // do something smart
        // return;
        // }
        // noinspection SuspiciousSystemArraycopy
        System.arraycopy(other.ring, 0, ring, 0, capacity);
    }

    abstract void clear();

    abstract void fillKey(@NotNull WritableChunk<? super Values> destination, int destOffset, int ix);

    abstract T get(long key);

    Boolean getBoolean(long key) {
        throw new UnsupportedOperationException();
    }

    byte getByte(long key) {
        throw new UnsupportedOperationException();
    }

    char getChar(long key) {
        throw new UnsupportedOperationException();
    }

    double getDouble(long key) {
        throw new UnsupportedOperationException();
    }

    float getFloat(long key) {
        throw new UnsupportedOperationException();
    }

    int getInt(long key) {
        throw new UnsupportedOperationException();
    }

    long getLong(long key) {
        throw new UnsupportedOperationException();
    }

    short getShort(long key) {
        throw new UnsupportedOperationException();
    }
}
