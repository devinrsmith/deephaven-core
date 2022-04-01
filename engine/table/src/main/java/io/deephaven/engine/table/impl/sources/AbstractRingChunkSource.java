package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.table.impl.DefaultChunkSource;
import io.deephaven.engine.table.impl.DefaultGetContext;
import io.deephaven.util.datastructures.LongRangeConsumer;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;
import java.util.Objects;
import java.util.function.LongConsumer;

import static io.deephaven.engine.table.impl.AbstractColumnSource.USE_RANGES_AVERAGE_RUN_LENGTH;

public abstract class AbstractRingChunkSource<T, ARRAY, SELF extends AbstractRingChunkSource<T, ARRAY, SELF>> implements DefaultChunkSource<Values> {

    protected final ARRAY ring;
    protected final int n;
    private long nextIx;

    public AbstractRingChunkSource(@NotNull Class<T> componentType, int n) {
        this.n = n;
        //noinspection unchecked
        ring = (ARRAY) Array.newInstance(componentType, n);
    }

    public final int n() {
        return n;
    }

    public final int size() {
        return n <= nextIx ? n : (int) nextIx;
    }

    public final boolean isEmpty() {
        return nextIx == 0;
    }

    public boolean containsIndex(long key) {
        return key >= 0 && key >= (nextIx - n) && key < nextIx;
    }

    public OrderedLongSet indices() {
        return isEmpty() ? OrderedLongSet.EMPTY : SingleRange.make(nextIx - size(), nextIx - 1);
    }



    public void appendFromChunk(Chunk<?> src, int srcOffset) {



    }

    @Override
    public final Chunk<Values> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        final int firstIx = (int) (firstKey % n);
        final int lastIx = (int) (lastKey % n);
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
            final int len = fillChunk(chunk, 0, firstIx, lastIx);
            chunk.setSize(len);
            return chunk;
        }
    }

    @Override
    public final void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
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
            final int ix = (int) (key % n);
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
            final int firstIx = (int) (firstKey % n);
            final int lastIx = (int) (lastKey % n);
            final int len = fillChunk(destination, destOffset, firstIx, lastIx);
            destOffset += len;
        }
    }

    private int fillChunk(@NotNull WritableChunk<? super Values> destination, int destOffset, int firstIx, int lastIx) {
        if (firstIx <= lastIx) {
            final int len = (lastIx - firstIx) + 1;
            destination.copyFromArray(ring, firstIx, destOffset, len);
            return len;
        } else {
            final int fill1Length = n - firstIx;
            final int fill2Length = lastIx + 1;
            destination.copyFromArray(ring, firstIx, destOffset, fill1Length);
            destination.copyFromArray(ring, 0, destOffset + fill1Length, fill2Length);
            return fill1Length + fill2Length;
        }
    }

    abstract void fillKey(@NotNull WritableChunk<? super Values> destination, int destOffset, int ix);

    abstract T get(long key);

    final int keyToRingIndex(long key) {
        return (int) (key % n);
    }

    void copyFrom(SELF other) {
        //noinspection SuspiciousSystemArraycopy
        System.arraycopy(other.ring, 0, ring, 0, n);
    }

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
