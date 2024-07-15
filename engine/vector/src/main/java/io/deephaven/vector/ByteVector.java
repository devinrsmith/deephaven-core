//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVector and run "./gradlew replicateVectors" to regenerate
//
// @formatter:off
package io.deephaven.vector;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfByte;
import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.compare.ByteComparisons;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A {@link Vector} of primitive bytes.
 */
public interface ByteVector extends Vector<ByteVector>, Iterable<Byte> {

    long serialVersionUID = -1373264425081841175L;

    static PrimitiveVectorType<ByteVector, Byte> type() {
        return PrimitiveVectorType.of(ByteVector.class, ByteType.of());
    }

    /**
     * Get the element of this ByteVector at offset {@code index}. If {@code index} is not within range
     * {@code [0, size())}, will return the {@link QueryConstants#NULL_BYTE null byte}.
     *
     * @param index An offset into this ByteVector
     * @return The element at the specified offset, or the {@link QueryConstants#NULL_BYTE null byte}
     */
    byte get(long index);

    @Override
    ByteVector subVector(long fromIndexInclusive, long toIndexExclusive);

    @Override
    ByteVector subVectorByPositions(long[] positions);

    @Override
    byte[] toArray();

    @Override
    byte[] copyToArray();

    @Override
    ByteVector getDirect();

    /**
     * Logically equivalent to {@code other != null && ByteComparisons.eq(toArray(), other.toArray())}.
     *
     * @param other the other vector
     * @return {@code true} if the two vectors are equal
     * @see ByteComparisons#eq(byte[], byte[])
     */
    @Override
    boolean equals(@Nullable ByteVector other);

    /**
     * Logically equivalent to {@code ByteComparisons.hashCode(toArray())}.
     *
     * @return the hash code value for this vector
     * @see ByteComparisons#hashCode(byte[])
     */
    @Override
    int hashCode();

    @Override
    @FinalDefault
    default CloseablePrimitiveIteratorOfByte iterator() {
        return iterator(0, size());
    }

    /**
     * Returns an iterator over a slice of this vector, with equivalent semantics to
     * {@code subVector(fromIndexInclusive, toIndexExclusive).iterator()}.
     *
     * @param fromIndexInclusive The first position to include
     * @param toIndexExclusive The first position after {@code fromIndexInclusive} to not include
     * @return An iterator over the requested slice
     */
    default CloseablePrimitiveIteratorOfByte iterator(final long fromIndexInclusive, final long toIndexExclusive) {
        Require.leq(fromIndexInclusive, "fromIndexInclusive", toIndexExclusive, "toIndexExclusive");
        return new CloseablePrimitiveIteratorOfByte() {

            long nextIndex = fromIndexInclusive;

            @Override
            public byte nextByte() {
                return get(nextIndex++);
            }

            @Override
            public boolean hasNext() {
                return nextIndex < toIndexExclusive;
            }
        };
    }

    @Override
    @FinalDefault
    default Class<?> getComponentType() {
        return byte.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    static String byteValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveByteValToString((Byte) val);
    }

    static String primitiveByteValToString(final byte val) {
        return val == QueryConstants.NULL_BYTE ? NULL_ELEMENT_STRING : Byte.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param vector The ByteVector to convert to a String
     * @param prefixLength The maximum prefix of {@code vector} to convert
     * @return The String representation of {@code vector}
     */
    static String toString(@NotNull final ByteVector vector, final int prefixLength) {
        if (vector.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(vector.size(), prefixLength);
        try (final CloseablePrimitiveIteratorOfByte iterator = vector.iterator(0, displaySize)) {
            builder.append(primitiveByteValToString(iterator.nextByte()));
            iterator.forEachRemaining(
                    (final byte value) -> builder.append(',').append(primitiveByteValToString(value)));
        }
        if (displaySize == vector.size()) {
            builder.append(']');
        } else {
            builder.append(", ...]");
        }
        return builder.toString();
    }

    /**
     * Helper method for implementing {@link ByteVector#equals(ByteVector)}. Two vectors are considered equal if both
     * vectors are the same {@link ByteVector#size() size} and all corresponding pairs of elements in the two vectors
     * are {@link ByteComparisons#eq(byte, byte)}.
     *
     * @param aVector The LHS of the equality test
     * @param bVector The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final ByteVector aVector, @NotNull final ByteVector bVector) {
        if (aVector == bVector) {
            return true;
        }
        final long size = aVector.size();
        if (size != bVector.size()) {
            return false;
        }
        if (size == 0) {
            return true;
        }
        try (final CloseablePrimitiveIteratorOfByte aIterator = aVector.iterator();
                final CloseablePrimitiveIteratorOfByte bIterator = bVector.iterator()) {
            while (aIterator.hasNext()) {
                if (!ByteComparisons.eq(aIterator.nextByte(), bIterator.nextByte())) {
                    return false;
                }
            }
            if (bIterator.hasNext()) {
                throw new IllegalStateException("Vector size / iterator mismatch, expected iterator to be exhausted");
            }
        }
        return true;
    }

    /**
     * Helper method for implementing {@link ByteVector#hashCode()}. An iterative equivalent of
     * {@code ByteComparisons.hashCode(vector.toArray())}.
     *
     * @param vector The ByteVector to hash
     * @return The hash code
     * @see ByteComparisons#hashCode(byte[])
     */
    static int hashCode(@NotNull final ByteVector vector) {
        int result = 1;
        if (vector.isEmpty()) {
            return result;
        }
        try (final CloseablePrimitiveIteratorOfByte iterator = vector.iterator()) {
            while (iterator.hasNext()) {
                result = 31 * result + ByteComparisons.hashCode(iterator.nextByte());
            }
        }
        return result;
    }

    /**
     * Base class for all "indirect" ByteVector implementations.
     */
    abstract class Indirect implements ByteVector {

        @Override
        public byte[] toArray() {
            return copyToArray();
        }

        @Override
        public byte[] copyToArray() {
            final int size = intSize("ByteVector.copyToArray");
            final byte[] result = new byte[size];
            try (final CloseablePrimitiveIteratorOfByte iterator = iterator()) {
                for (int ei = 0; ei < size; ++ei) {
                    result[ei] = iterator.nextByte();
                }
            }
            return result;
        }

        @Override
        public ByteVector getDirect() {
            return new ByteVectorDirect(copyToArray());
        }

        @Override
        public final String toString() {
            return ByteVector.toString(this, 10);
        }

        @Override
        public final boolean equals(Object other) {
            return other instanceof ByteVector && ByteVector.equals(this, (ByteVector) other);
        }

        @Override
        public final boolean equals(@Nullable ByteVector other) {
            return other != null && ByteVector.equals(this, other);
        }

        @Override
        public final int hashCode() {
            return ByteVector.hashCode(this);
        }

        protected final Object writeReplace() {
            return getDirect();
        }
    }
}
