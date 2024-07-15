//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVector and run "./gradlew replicateVectors" to regenerate
//
// @formatter:off
package io.deephaven.vector;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfLong;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.compare.LongComparisons;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A {@link Vector} of primitive longs.
 */
public interface LongVector extends Vector<LongVector>, Iterable<Long> {

    long serialVersionUID = -1373264425081841175L;

    static PrimitiveVectorType<LongVector, Long> type() {
        return PrimitiveVectorType.of(LongVector.class, LongType.of());
    }

    /**
     * Get the element of this LongVector at offset {@code index}. If {@code index} is not within range
     * {@code [0, size())}, will return the {@link QueryConstants#NULL_LONG null long}.
     *
     * @param index An offset into this LongVector
     * @return The element at the specified offset, or the {@link QueryConstants#NULL_LONG null long}
     */
    long get(long index);

    @Override
    LongVector subVector(long fromIndexInclusive, long toIndexExclusive);

    @Override
    LongVector subVectorByPositions(long[] positions);

    @Override
    long[] toArray();

    @Override
    long[] copyToArray();

    @Override
    LongVector getDirect();

    /**
     * Logically equivalent to {@code other != null && LongComparisons.eq(toArray(), other.toArray())}.
     *
     * @param other the other vector
     * @return {@code true} if the two vectors are equal
     * @see LongComparisons#eq(long[], long[])
     */
    @Override
    boolean equals(@Nullable LongVector other);

    /**
     * Logically equivalent to {@code LongComparisons.hashCode(toArray())}.
     *
     * @return the hash code value for this vector
     * @see LongComparisons#hashCode(long[])
     */
    @Override
    int hashCode();

    @Override
    @FinalDefault
    default CloseablePrimitiveIteratorOfLong iterator() {
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
    default CloseablePrimitiveIteratorOfLong iterator(final long fromIndexInclusive, final long toIndexExclusive) {
        Require.leq(fromIndexInclusive, "fromIndexInclusive", toIndexExclusive, "toIndexExclusive");
        return new CloseablePrimitiveIteratorOfLong() {

            long nextIndex = fromIndexInclusive;

            @Override
            public long nextLong() {
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
        return long.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    static String longValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveLongValToString((Long) val);
    }

    static String primitiveLongValToString(final long val) {
        return val == QueryConstants.NULL_LONG ? NULL_ELEMENT_STRING : Long.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param vector The LongVector to convert to a String
     * @param prefixLength The maximum prefix of {@code vector} to convert
     * @return The String representation of {@code vector}
     */
    static String toString(@NotNull final LongVector vector, final int prefixLength) {
        if (vector.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(vector.size(), prefixLength);
        try (final CloseablePrimitiveIteratorOfLong iterator = vector.iterator(0, displaySize)) {
            builder.append(primitiveLongValToString(iterator.nextLong()));
            iterator.forEachRemaining(
                    (final long value) -> builder.append(',').append(primitiveLongValToString(value)));
        }
        if (displaySize == vector.size()) {
            builder.append(']');
        } else {
            builder.append(", ...]");
        }
        return builder.toString();
    }

    /**
     * Helper method for implementing {@link LongVector#equals(LongVector)}. Two vectors are considered equal if both
     * vectors are the same {@link LongVector#size() size} and all corresponding pairs of elements in the two vectors
     * are {@link LongComparisons#eq(long, long)}.
     *
     * @param aVector The LHS of the equality test
     * @param bVector The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final LongVector aVector, @NotNull final LongVector bVector) {
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
        try (final CloseablePrimitiveIteratorOfLong aIterator = aVector.iterator();
                final CloseablePrimitiveIteratorOfLong bIterator = bVector.iterator()) {
            while (aIterator.hasNext()) {
                if (!LongComparisons.eq(aIterator.nextLong(), bIterator.nextLong())) {
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
     * Helper method for implementing {@link LongVector#hashCode()}. An iterative equivalent of
     * {@code LongComparisons.hashCode(vector.toArray())}.
     *
     * @param vector The LongVector to hash
     * @return The hash code
     * @see LongComparisons#hashCode(long[])
     */
    static int hashCode(@NotNull final LongVector vector) {
        int result = 1;
        if (vector.isEmpty()) {
            return result;
        }
        try (final CloseablePrimitiveIteratorOfLong iterator = vector.iterator()) {
            while (iterator.hasNext()) {
                result = 31 * result + LongComparisons.hashCode(iterator.nextLong());
            }
        }
        return result;
    }

    /**
     * Base class for all "indirect" LongVector implementations.
     */
    abstract class Indirect implements LongVector {

        @Override
        public long[] toArray() {
            return copyToArray();
        }

        @Override
        public long[] copyToArray() {
            final int size = intSize("LongVector.copyToArray");
            final long[] result = new long[size];
            try (final CloseablePrimitiveIteratorOfLong iterator = iterator()) {
                for (int ei = 0; ei < size; ++ei) {
                    result[ei] = iterator.nextLong();
                }
            }
            return result;
        }

        @Override
        public LongVector getDirect() {
            return new LongVectorDirect(copyToArray());
        }

        @Override
        public final String toString() {
            return LongVector.toString(this, 10);
        }

        @Override
        public final boolean equals(Object other) {
            return other instanceof LongVector && LongVector.equals(this, (LongVector) other);
        }

        @Override
        public final boolean equals(@Nullable LongVector other) {
            return other != null && LongVector.equals(this, other);
        }

        @Override
        public final int hashCode() {
            return LongVector.hashCode(this);
        }

        protected final Object writeReplace() {
            return getDirect();
        }
    }
}
