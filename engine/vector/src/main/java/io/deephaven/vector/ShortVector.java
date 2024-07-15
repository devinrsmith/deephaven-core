//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVector and run "./gradlew replicateVectors" to regenerate
//
// @formatter:off
package io.deephaven.vector;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfShort;
import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.compare.ShortComparisons;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A {@link Vector} of primitive shorts.
 */
public interface ShortVector extends Vector<ShortVector>, Iterable<Short> {

    long serialVersionUID = -1373264425081841175L;

    static PrimitiveVectorType<ShortVector, Short> type() {
        return PrimitiveVectorType.of(ShortVector.class, ShortType.of());
    }

    /**
     * Get the element of this ShortVector at offset {@code index}. If {@code index} is not within range
     * {@code [0, size())}, will return the {@link QueryConstants#NULL_SHORT null short}.
     *
     * @param index An offset into this ShortVector
     * @return The element at the specified offset, or the {@link QueryConstants#NULL_SHORT null short}
     */
    short get(long index);

    @Override
    ShortVector subVector(long fromIndexInclusive, long toIndexExclusive);

    @Override
    ShortVector subVectorByPositions(long[] positions);

    @Override
    short[] toArray();

    @Override
    short[] copyToArray();

    @Override
    ShortVector getDirect();

    /**
     * Logically equivalent to {@code other != null && ShortComparisons.eq(toArray(), other.toArray())}.
     *
     * @param other the other vector
     * @return {@code true} if the two vectors are equal
     * @see ShortComparisons#eq(short[], short[])
     */
    @Override
    boolean equals(@Nullable ShortVector other);

    /**
     * Logically equivalent to {@code ShortComparisons.hashCode(toArray())}.
     *
     * @return the hash code value for this vector
     * @see ShortComparisons#hashCode(short[])
     */
    @Override
    int hashCode();

    @Override
    @FinalDefault
    default CloseablePrimitiveIteratorOfShort iterator() {
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
    default CloseablePrimitiveIteratorOfShort iterator(final long fromIndexInclusive, final long toIndexExclusive) {
        Require.leq(fromIndexInclusive, "fromIndexInclusive", toIndexExclusive, "toIndexExclusive");
        return new CloseablePrimitiveIteratorOfShort() {

            long nextIndex = fromIndexInclusive;

            @Override
            public short nextShort() {
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
        return short.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    static String shortValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveShortValToString((Short) val);
    }

    static String primitiveShortValToString(final short val) {
        return val == QueryConstants.NULL_SHORT ? NULL_ELEMENT_STRING : Short.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param vector The ShortVector to convert to a String
     * @param prefixLength The maximum prefix of {@code vector} to convert
     * @return The String representation of {@code vector}
     */
    static String toString(@NotNull final ShortVector vector, final int prefixLength) {
        if (vector.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(vector.size(), prefixLength);
        try (final CloseablePrimitiveIteratorOfShort iterator = vector.iterator(0, displaySize)) {
            builder.append(primitiveShortValToString(iterator.nextShort()));
            iterator.forEachRemaining(
                    (final short value) -> builder.append(',').append(primitiveShortValToString(value)));
        }
        if (displaySize == vector.size()) {
            builder.append(']');
        } else {
            builder.append(", ...]");
        }
        return builder.toString();
    }

    /**
     * Helper method for implementing {@link ShortVector#equals(ShortVector)}. Two vectors are considered equal if both
     * vectors are the same {@link ShortVector#size() size} and all corresponding pairs of elements in the two vectors
     * are {@link ShortComparisons#eq(short, short)}.
     *
     * @param aVector The LHS of the equality test
     * @param bVector The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final ShortVector aVector, @NotNull final ShortVector bVector) {
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
        try (final CloseablePrimitiveIteratorOfShort aIterator = aVector.iterator();
                final CloseablePrimitiveIteratorOfShort bIterator = bVector.iterator()) {
            while (aIterator.hasNext()) {
                if (!ShortComparisons.eq(aIterator.nextShort(), bIterator.nextShort())) {
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
     * Helper method for implementing {@link ShortVector#hashCode()}. An iterative equivalent of
     * {@code ShortComparisons.hashCode(vector.toArray())}.
     *
     * @param vector The ShortVector to hash
     * @return The hash code
     * @see ShortComparisons#hashCode(short[])
     */
    static int hashCode(@NotNull final ShortVector vector) {
        int result = 1;
        if (vector.isEmpty()) {
            return result;
        }
        try (final CloseablePrimitiveIteratorOfShort iterator = vector.iterator()) {
            while (iterator.hasNext()) {
                result = 31 * result + ShortComparisons.hashCode(iterator.nextShort());
            }
        }
        return result;
    }

    /**
     * Base class for all "indirect" ShortVector implementations.
     */
    abstract class Indirect implements ShortVector {

        @Override
        public short[] toArray() {
            return copyToArray();
        }

        @Override
        public short[] copyToArray() {
            final int size = intSize("ShortVector.copyToArray");
            final short[] result = new short[size];
            try (final CloseablePrimitiveIteratorOfShort iterator = iterator()) {
                for (int ei = 0; ei < size; ++ei) {
                    result[ei] = iterator.nextShort();
                }
            }
            return result;
        }

        @Override
        public ShortVector getDirect() {
            return new ShortVectorDirect(copyToArray());
        }

        @Override
        public final String toString() {
            return ShortVector.toString(this, 10);
        }

        @Override
        public final boolean equals(Object other) {
            return other instanceof ShortVector && ShortVector.equals(this, (ShortVector) other);
        }

        @Override
        public final boolean equals(@Nullable ShortVector other) {
            return other != null && ShortVector.equals(this, other);
        }

        @Override
        public final int hashCode() {
            return ShortVector.hashCode(this);
        }

        protected final Object writeReplace() {
            return getDirect();
        }
    }
}
