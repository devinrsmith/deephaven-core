/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharacterArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.DefaultGetContext;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.sources.chunkcolumnsource.DoubleChunkColumnSource.ChunkGetContext;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;


public final class DoubleArrayRingSource
        extends AbstractColumnSource<Double>
    // todo ChunkedBackingStoreExposedWritableSource?
        implements MutableColumnSourceGetDefaults.ForDouble, InMemoryColumnSource {

    private final double[] buffer;
    private final double[] prevBuffer;
    private long nextIx;
    private long prevIx;

    public DoubleArrayRingSource(int n) {
        super(double.class);
        if (n <= 0) {
            throw new IllegalArgumentException("n must be positive");
        }
        buffer = new double[n];
        prevBuffer = new double[n];
        nextIx = 0;
    }

    /**
     * The size. Less than, or equal to, {@link #n()}. The size will never shrink.
     *
     * @return the size
     */
    public int size() {
        return buffer.length <= nextIx ? buffer.length : (int) nextIx;
    }

    public int prevSize() {
        return prevBuffer.length <= prevIx ? prevBuffer.length : (int) prevIx;
    }

    /**
     *
     * @return is empty
     */
    public boolean isEmpty() {
        return nextIx == 0;
    }

    public boolean prevIsEmpty() {
        return prevIx == 0;
    }

    /**
     * The maximum number of elements in this ring.
     *
     * @return n
     */
    public int n() {
        return buffer.length;
    }

    public boolean containsIndex(long index) {
        return index >= 0 && index >= (nextIx - buffer.length) && index < nextIx;
    }

    public boolean prevContainsIndex(long index) {
        return index >= 0 && index >= (prevIx - prevBuffer.length) && index < prevIx;
    }

    public OrderedLongSet indices() {
        return isEmpty() ? OrderedLongSet.EMPTY : SingleRange.make(nextIx - size(), nextIx - 1);
    }

    public OrderedLongSet prevIndices() {
        return prevIsEmpty() ? OrderedLongSet.EMPTY : SingleRange.make(prevIx - prevSize(), prevIx - 1);
    }

    public void bringPreviousUpToDate() {
        System.arraycopy(buffer, 0, prevBuffer, 0, buffer.length);
        prevIx = nextIx;
    }

    public void add(double x) {
        final long localIx = nextIx;
        final int bufferIx = (int) (localIx % buffer.length);
        buffer[bufferIx] = x;
        nextIx = localIx + 1;
    }

    public void add(double[] xs) {
        add(xs, 0, xs.length);
    }

    public void add(double[] xs, int offset, int len) {
        if (offset < 0 || len < 0 || offset > xs.length - len) {
            throw new IndexOutOfBoundsException();
        }
        final int N = size();
        final long localIx;
        final int actualOffset;
        final int actualLen;
        if (len <= N) {
            localIx = nextIx;
            actualOffset = offset;
            actualLen = len;
        } else {
            final int extraOffset = len - N;
            localIx = nextIx + extraOffset;
            actualOffset = offset + extraOffset;
            actualLen = N;
        }

        // [0, size())
        final int copy1Start = (int) (localIx % buffer.length);

        // (0, size()]
        final int copy1Max = buffer.length - copy1Start;

        // copy1Len + copy2Len = len
        final int copy1Len = Math.min(copy1Max, actualLen);
        final int copy2Len = actualLen - copy1Len;

        System.arraycopy(xs, actualOffset, buffer, copy1Start, copy1Len);
        System.arraycopy(xs, actualOffset + copy1Len, buffer, 0, copy2Len);
        nextIx = localIx + actualLen;
    }

    @Override
    public double getDouble(long index) {
        if (!containsIndex(index)) {
            return NULL_DOUBLE;
        }
        return getDoubleUnsafe(index);
    }

    @Override
    public double getPrevDouble(long index) {
        if (!prevContainsIndex(index)) {
            return NULL_DOUBLE;
        }
        return prevGetDoubleUnsafe(index);
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        return super.getChunk(context, rowSequence);
    }

    @Override
    public Chunk<Values> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        final int firstIx = (int) (firstKey % buffer.length);
        final int lastIx = (int) (lastKey % buffer.length);
        if (firstIx <= lastIx) {
            // Easy case, simple view!
//            return DoubleChunk.chunkWrap(buffer, firstIx, (lastIx - firstIx) + 1);
            return DefaultGetContext.resetChunkFromArray(context, buffer, firstIx, (lastIx - firstIx) + 1);
        } else {
            // Would be awesome if we could have a view of two wrapped DoubleChunks
            // final DoubleChunk<Any> c1 = DoubleChunk.chunkWrap(buffer, firstIx, buffer.length - firstIx);
            // final DoubleChunk<Any> c2 = DoubleChunk.chunkWrap(buffer, 0, lastIx + 1);
            // return view(c1, c2);

            DefaultGetContext.getFillContext(context);


            DoubleChunk.makeArray()

            ((ChunkGetContext) context).resettableDoubleChunk;

        }

    }

    @Override
    public Chunk<Values> getChunkByFilling(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        return super.getChunkByFilling(context, rowSequence);
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
        final WritableDoubleChunk<? super Values> c = destination.asWritableDoubleChunk();

        c.copyFromChunk();
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
        super.fillPrevChunk(context, destination, rowSequence);
    }

    public double getDoubleUnsafe(long index) {
        final int bufferIx = (int) (index % buffer.length);
        return buffer[bufferIx];
    }

    public double prevGetDoubleUnsafe(long index) {
        final int bufferIx = (int) (index % buffer.length);
        return prevBuffer[bufferIx];
    }

    //    public double getDoubleZero(int zeroIx) {
//        final int N = size();
//        if (zeroIx < 0 || zeroIx >= N) {
//            return NULL_DOUBLE;
//        }
//        final long startIx = endIx - N;
//        return getDoubleUnsafe(startIx + zeroIx);
//    }
//
//    public double getDoubleAgo(int agoIx) {
//        if (agoIx < 0 || agoIx >= size()) {
//            return NULL_DOUBLE;
//        }
//        return getDoubleUnsafe(endIx - agoIx);
//    }
}
