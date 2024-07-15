//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVectorDirect and run "./gradlew replicateVectors" to regenerate
//
// @formatter:off
package io.deephaven.vector;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfByte;
import io.deephaven.util.annotations.ArrayType;
import io.deephaven.util.annotations.ArrayTypeGetter;
import io.deephaven.util.compare.ByteComparisons;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_BYTE;

/**
 * A {@link ByteVector} backed by an array.
 */
@ArrayType(type = byte[].class)
public final class ByteVectorDirect implements ByteVector {

    private final static long serialVersionUID = 3636374971797603565L;

    public static final ByteVector ZERO_LENGTH_VECTOR = new ByteVectorDirect();

    private final byte[] data;

    public ByteVectorDirect(@NotNull final byte... data) {
        this.data = Require.neqNull(data, "data");
    }

    @Override
    public byte get(final long index) {
        if (index < 0 || index >= data.length) {
            return NULL_BYTE;
        }
        return data[(int) index];
    }

    @Override
    public ByteVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return new ByteVectorSlice(this, fromIndexInclusive, toIndexExclusive - fromIndexInclusive);
    }

    public ByteVector subVectorByPositions(final long[] positions) {
        return new ByteSubVector(this, positions);
    }

    @Override
    @ArrayTypeGetter
    public byte[] toArray() {
        return data;
    }

    @Override
    public byte[] copyToArray() {
        return Arrays.copyOf(data, data.length);
    }

    public byte[] copyOfRange(int from, int to) {
        return Arrays.copyOfRange(data, from, to);
    }

    @Override
    public CloseablePrimitiveIteratorOfByte iterator(final long fromIndexInclusive, final long toIndexExclusive) {
        if (fromIndexInclusive == 0 && toIndexExclusive == data.length) {
            return CloseablePrimitiveIteratorOfByte.of(data);
        }
        return ByteVector.super.iterator(fromIndexInclusive, toIndexExclusive);
    }

    @Override
    public long size() {
        return data.length;
    }

    @Override
    public ByteVectorDirect getDirect() {
        return this;
    }

    @Override
    public String toString() {
        return ByteVector.toString(this, 10);
    }

    @Override
    public boolean equals(final Object obj) {
        return obj instanceof ByteVector && equals((ByteVector) obj);
    }

    @Override
    public boolean equals(@Nullable ByteVector other) {
        if (other == null) {
            return false;
        }
        return other instanceof ByteVectorDirect
                ? ByteComparisons.eq(data, ((ByteVectorDirect) other).data)
                : ByteVector.equals(this, other);
    }

    @Override
    public int hashCode() {
        return ByteComparisons.hashCode(data);
    }
}
