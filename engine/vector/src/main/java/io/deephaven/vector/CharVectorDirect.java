//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.vector;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfChar;
import io.deephaven.util.annotations.ArrayType;
import io.deephaven.util.annotations.ArrayTypeGetter;
import io.deephaven.util.compare.CharComparisons;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

/**
 * A {@link CharVector} backed by an array.
 */
@ArrayType(type = char[].class)
public final class CharVectorDirect implements CharVector {

    private final static long serialVersionUID = 3636374971797603565L;

    public static final CharVector ZERO_LENGTH_VECTOR = new CharVectorDirect();

    private final char[] data;

    public CharVectorDirect(@NotNull final char... data) {
        this.data = Require.neqNull(data, "data");
    }

    @Override
    public char get(final long index) {
        if (index < 0 || index >= data.length) {
            return NULL_CHAR;
        }
        return data[(int) index];
    }

    @Override
    public CharVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return new CharVectorSlice(this, fromIndexInclusive, toIndexExclusive - fromIndexInclusive);
    }

    public CharVector subVectorByPositions(final long[] positions) {
        return new CharSubVector(this, positions);
    }

    @Override
    @ArrayTypeGetter
    public char[] toArray() {
        return data;
    }

    @Override
    public char[] copyToArray() {
        return Arrays.copyOf(data, data.length);
    }

    public char[] copyOfRange(int from, int to) {
        return Arrays.copyOfRange(data, from, to);
    }

    @Override
    public CloseablePrimitiveIteratorOfChar iterator(final long fromIndexInclusive, final long toIndexExclusive) {
        if (fromIndexInclusive == 0 && toIndexExclusive == data.length) {
            return CloseablePrimitiveIteratorOfChar.of(data);
        }
        return CharVector.super.iterator(fromIndexInclusive, toIndexExclusive);
    }

    @Override
    public long size() {
        return data.length;
    }

    @Override
    public CharVectorDirect getDirect() {
        return this;
    }

    @Override
    public String toString() {
        return CharVector.toString(this, 10);
    }

    @Override
    public boolean equals(final Object obj) {
        return obj instanceof CharVector && equals((CharVector) obj);
    }

    @Override
    public boolean equals(@Nullable CharVector other) {
        if (other == null) {
            return false;
        }
        return other instanceof CharVectorDirect
                ? CharComparisons.eq(data, ((CharVectorDirect) other).data)
                : CharVector.equals(this, other);
    }

    @Override
    public int hashCode() {
        return CharComparisons.hashCode(data);
    }
}
