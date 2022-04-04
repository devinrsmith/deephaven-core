package io.deephaven.engine.table.impl.sources.ring;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.impl.DefaultChunkSource;
import io.deephaven.engine.table.impl.DefaultGetContext;
import io.deephaven.util.datastructures.LongRangeConsumer;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.lang.reflect.Array;
import java.util.Objects;
import java.util.function.LongConsumer;

import static io.deephaven.engine.table.impl.AbstractColumnSource.USE_RANGES_AVERAGE_RUN_LENGTH;

abstract class AbstractRingChunkSource<T, ARRAY, SELF extends AbstractRingChunkSource<T, ARRAY, SELF>>
        implements DefaultChunkSource<Values> {
    // todo: should we extend SupportsContiguousGet? I think the extra layers of default methods may hurt

    // todo: should this be a (writable)chunk?
    protected final ARRAY ring;
    protected final int capacity;
    long nextKey;

    public AbstractRingChunkSource(@NotNull Class<T> componentType, int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive");
        }
        this.capacity = capacity;
        // noinspection unchecked
        ring = (ARRAY) Array.newInstance(componentType, capacity);
    }

    /**
     * The maximum size {@code this} ring can hold. Constant.
     *
     * @return the capacity
     */
    public final int capacity() {
        return capacity;
    }

    /**
     * The size, {@code 0 <= size <= capacity}. The size will never shrink.
     *
     * <p>
     * Logically equivalent to {@code lastKey - firstKey + 1}.
     *
     * @return the size
     */
    public final int size() {
        return capacity <= nextKey ? capacity : (int) nextKey;
    }

    /**
     * {@code true} if empty, else {@code false}. Once {@code false} is returned, will always return {@code false}.
     *
     * <p>
     * Logically equivalent to {@code size == 0}.
     *
     * @return {@code true} if empty
     */
    public final boolean isEmpty() {
        return nextKey == 0;
    }

    /**
     * {@code true} if {@code key} is in the index.
     *
     * <p>
     * Logically equivalent to the condition {@code firstKey <= key <= lastKey}.
     *
     * @param key the key
     * @return {@code true} if {@code key} is in the index.
     * @see #firstKey()
     * @see #lastKey()
     */
    public final boolean containsKey(long key) {
        // branchless check, probably better than needing to compute size() or firstKey()
        return key >= 0 && key >= (nextKey - capacity) && key < nextKey;
    }

    public final boolean containsRange(long firstKey, long lastKey) {
        return firstKey <= lastKey && firstKey >= 0 && firstKey >= (nextKey - capacity) && lastKey < nextKey;
    }

    /**
     * The first key (inclusive). If {@link #isEmpty()}, returns {@code 0}.
     *
     * @return the first key
     * @see #lastKey()
     */
    public final long firstKey() {
        return capacity <= nextKey ? nextKey - capacity : 0;
    }

    /**
     * The last key (inclusive). If {@link #isEmpty()}, returns {@code -1}.
     *
     * @return the last key
     * @see #firstKey()
     */
    public final long lastKey() {
        return nextKey - 1;
    }

    // todo: if we can get efficient last N, we can implement this w/ RowSequence
    // not absolutely necessary since we are only filling from a stream table atm
    public final void append(
            ChunkSource<? extends Values> src, FillContext fillContext, GetContext context, long firstKey, long lastKey) {
        // todo: should we have our own get context?

        if (firstKey < 0) {
            throw new IllegalArgumentException("todo");
        }
        if (firstKey > lastKey) {
            throw new IllegalArgumentException("Need at least one element to append");
        }
        final long logicalFillSize = lastKey - firstKey + 1;
        final long physicalStartKey;
        final long modifiedFirstKey;
        final int physicalFillSize;
        if (logicalFillSize <= capacity) {
            physicalStartKey = nextKey;
            modifiedFirstKey = firstKey;
            physicalFillSize = (int) logicalFillSize;
        } else {
            final long skippedElements = logicalFillSize - capacity;
            physicalStartKey = nextKey + skippedElements;
            modifiedFirstKey = firstKey + skippedElements;
            physicalFillSize = capacity;
        }

        // [0, capacity)
        final int fillIndex1 = keyToRingIndex(physicalStartKey);
        // (0, capacity]
        final int fillMax1 = capacity - fillIndex1;

        // fillSize1 + fillSize2 = physicalFillSize
        final int fillSize1 = Math.min(fillMax1, physicalFillSize);
        final int fillSize2 = physicalFillSize - fillSize1;

        final long secondKey = modifiedFirstKey + fillSize1;

        // todo: why does the factory not use SingleRangeRowSequence
        // todo: should there be a specialized fillChunk w/ range?
        {
            final RowSequence seq1 = RowSequenceFactory.forRange(modifiedFirstKey, secondKey - 1);
            final WritableChunk<Values> chunk = DefaultGetContext
                    .getResettableChunk(context)
                    .resetFromArray(ring, fillIndex1, fillSize1);
            src.fillChunk(fillContext, chunk, seq1);
        }
        if (fillSize2 != 0) {
            final RowSequence seq2 = RowSequenceFactory.forRange(secondKey, lastKey);
            final WritableChunk<Values> chunk = DefaultGetContext
                    .getResettableChunk(context)
                    .resetFromArray(ring, 0, fillSize2);
            src.fillChunk(fillContext, chunk, seq2);
        }
        nextKey = physicalStartKey + physicalFillSize;
    }

    @Override
    public final Chunk<Values> getChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        if (rowSequence.isEmpty()) {
            return getChunkType().getEmptyChunk();
        }
        if (rowSequence.isContiguous()) {
            return getChunk(context, rowSequence.firstRowKey(), rowSequence.lastRowKey());
        }
        final WritableChunk<Values> chunk = DefaultGetContext.getWritableChunk(context);
        fillChunk(DefaultGetContext.getFillContext(context), chunk, rowSequence);
        return chunk;
    }

    @Override
    public final Chunk<Values> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        // This check should not be necessary given precondition on getChunk
        if (!containsRange(firstKey, lastKey)) {
            throw new IllegalStateException("getChunk precondition broken, invalid range");
        }
        final int firstRingIx = keyToRingIndex(firstKey);
        final int secondRingIx = keyToRingIndex(lastKey);
        if (firstRingIx <= secondRingIx) {
            // Easy case, simple view.
            // More efficient than DefaultGetContext.resetChunkFromArray since we known ring != null
            return DefaultGetContext
                    .getResettableChunk(context)
                    .resetFromArray(ring, firstRingIx, secondRingIx - firstRingIx + 1);
        } else {
            // Would be awesome if we could have a view of two wrapped DoubleChunks
            // final DoubleChunk<Any> c1 = DoubleChunk.chunkWrap(buffer, firstRingIx, buffer.length - firstRingIx);
            // final DoubleChunk<Any> c2 = DoubleChunk.chunkWrap(buffer, 0, secondRingIx + 1);
            // return view(c1, c2);
            final WritableChunk<Values> chunk = DefaultGetContext.getWritableChunk(context);
            final int size = fillChunkByCopyFromArray(chunk, 0, firstRingIx, secondRingIx);
            if (size != lastKey - firstKey + 1) {
                throw new IllegalStateException();
            }
            chunk.setSize(size);
            return chunk;
        }
    }

    @Override
    public final void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        if (rowSequence.getAverageRunLengthEstimate() < USE_RANGES_AVERAGE_RUN_LENGTH) {
            try (final KeyFiller filler = new KeyFiller(destination)) {
                rowSequence.forAllRowKeys(filler);
            }
        } else {
            try (final RangeFiller filler = new RangeFiller(destination)) {
                rowSequence.forAllRowKeyRanges(filler);
            }
        }
    }

    private final class KeyFiller implements LongConsumer, Closeable {
        private final WritableChunk<? super Values> destination;
        private int destOffset;

        public KeyFiller(WritableChunk<? super Values> destination) {
            this.destination = Objects.requireNonNull(destination);
        }

        @Override
        public void accept(long key) {
            // this check should not be necessary given precondition on fillChunk
            if (!containsKey(key)) {
                throw new IllegalStateException("fillChunk precondition broken, invalid range");
            }
            final int ringIx = keyToRingIndex(key);
            fillKey(destination, destOffset, ringIx);
            ++destOffset;
        }

        @Override
        public void close() {
            destination.setSize(destOffset);
        }
    }

    private final class RangeFiller implements LongRangeConsumer, Closeable {
        private final WritableChunk<? super Values> destination;
        private int destOffset;

        public RangeFiller(WritableChunk<? super Values> destination) {
            this.destination = Objects.requireNonNull(destination);
        }

        @Override
        public void accept(long firstKey, long lastKey) {
            // this check should not be necessary given precondition on fillChunk
            if (!containsRange(firstKey, lastKey)) {
                throw new IllegalStateException("fillChunk precondition broken, invalid range");
            }
            final int firstRingIx = keyToRingIndex(firstKey);
            final int lastRingIx = keyToRingIndex(lastKey);
            final int size = fillChunkByCopyFromArray(destination, destOffset, firstRingIx, lastRingIx);
            if (size != lastKey - firstKey + 1) {
                throw new IllegalStateException();
            }
            destOffset += size;
        }

        @Override
        public void close() {
            destination.setSize(destOffset);
        }
    }

    private int fillChunkByCopyFromArray(@NotNull WritableChunk<? super Values> destination, int destOffset,
            int firstRingIx, int lastRingIx) {
        if (firstRingIx <= lastRingIx) {
            final int size = lastRingIx - firstRingIx + 1;
            destination.copyFromArray(ring, firstRingIx, destOffset, size);
            return size;
        } else {
            final int fillSize1 = capacity - firstRingIx;
            final int fillSize2 = lastRingIx + 1;
            destination.copyFromArray(ring, firstRingIx, destOffset, fillSize1);
            destination.copyFromArray(ring, 0, destOffset + fillSize1, fillSize2);
            return fillSize1 + fillSize2;
        }
    }

    final int keyToRingIndex(long key) {
        return (int) (key % capacity);
    }

    final void replayFrom(SELF other, FillContext fillContext, GetContext context) {
        final long logicalFillSize = other.nextKey - nextKey;
        if (logicalFillSize >= capacity / 2) {
            // noinspection SuspiciousSystemArraycopy
            System.arraycopy(other.ring, 0, ring, 0, capacity);
            nextKey = other.nextKey;
            return;
        }
        append(other, fillContext, context, nextKey, other.nextKey - 1);
        if (nextKey != other.nextKey) {
            throw new IllegalStateException();
        }
    }

    abstract void clear();

    abstract void fillKey(@NotNull WritableChunk<? super Values> destination, int destOffset, int ringIx);

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
