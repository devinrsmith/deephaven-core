/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharacterArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;


public final class DoubleArrayRing {

    private final double[] buffer;
    private long nextIx;

    public DoubleArrayRing(int n) {
        if (n <= 0) {
            throw new IllegalArgumentException("n must be positive");
        }
        buffer = new double[n];
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

    /**
     *
     * @return is empty
     */
    public boolean isEmpty() {
        return nextIx == 0;
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

    public OrderedLongSet indices() {
        return isEmpty() ? OrderedLongSet.EMPTY : SingleRange.make(nextIx - size(), nextIx - 1);
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

    public double getDouble(long index) {
        if (!containsIndex(index)) {
            return NULL_DOUBLE;
        }
        return getDoubleUnsafe(index);
    }

    public double getDoubleUnsafe(long index) {
        final int bufferIx = (int) (index % buffer.length);
        return buffer[bufferIx];
    }
}
