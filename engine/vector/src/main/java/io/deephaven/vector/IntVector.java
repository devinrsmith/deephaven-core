//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVector and run "./gradlew replicateVectors" to regenerate
//
// @formatter:off
package io.deephaven.vector;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfInt;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.compare.IntComparisons;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A {@link Vector} of primitive ints.
 */
public interface IntVector extends Vector<IntVector>, Iterable<Integer> {

    long serialVersionUID = -1373264425081841175L;

    static PrimitiveVectorType<IntVector, Integer> type() {
        return PrimitiveVectorType.of(IntVector.class, IntType.of());
    }

    /**
     * Get the element of this IntVector at offset {@code index}. If {@code index} is not within range
     * {@code [0, size())}, will return the {@link QueryConstants#NULL_INT null int}.
     *
     * @param index An offset into this IntVector
     * @return The element at the specified offset, or the {@link QueryConstants#NULL_INT null int}
     */
    int get(long index);

    @Override
    IntVector subVector(long fromIndexInclusive, long toIndexExclusive);

    @Override
    IntVector subVectorByPositions(long[] positions);

    @Override
    int[] toArray();

    @Override
    int[] copyToArray();

    @Override
    IntVector getDirect();

    /**
     * Logically equivalent to {@code other != null && IntComparisons.eq(toArray(), other.toArray())}.
     *
     * @param other the other vector
     * @return {@code true} if the two vectors are equal
     * @see IntComparisons#eq(int[], int[])
     */
    @Override
    boolean equals(@Nullable IntVector other);

    /**
     * Logically equivalent to {@code IntComparisons.hashCode(toArray())}.
     *
     * @return the hash code value for this vector
     * @see IntComparisons#hashCode(int[])
     */
    @Override
    int hashCode();

    @Override
    @FinalDefault
    default CloseablePrimitiveIteratorOfInt iterator() {
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
    default CloseablePrimitiveIteratorOfInt iterator(final long fromIndexInclusive, final long toIndexExclusive) {
        Require.leq(fromIndexInclusive, "fromIndexInclusive", toIndexExclusive, "toIndexExclusive");
        return new CloseablePrimitiveIteratorOfInt() {

            long nextIndex = fromIndexInclusive;

            @Override
            public int nextInt() {
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
        return int.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    static String intValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveIntValToString((Integer) val);
    }

    static String primitiveIntValToString(final int val) {
        return val == QueryConstants.NULL_INT ? NULL_ELEMENT_STRING : Integer.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param vector The IntVector to convert to a String
     * @param prefixLength The maximum prefix of {@code vector} to convert
     * @return The String representation of {@code vector}
     */
    static String toString(@NotNull final IntVector vector, final int prefixLength) {
        if (vector.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(vector.size(), prefixLength);
        try (final CloseablePrimitiveIteratorOfInt iterator = vector.iterator(0, displaySize)) {
            builder.append(primitiveIntValToString(iterator.nextInt()));
            iterator.forEachRemaining(
                    (final int value) -> builder.append(',').append(primitiveIntValToString(value)));
        }
        if (displaySize == vector.size()) {
            builder.append(']');
        } else {
            builder.append(", ...]");
        }
        return builder.toString();
    }

    /**
     * Helper method for implementing {@link IntVector#equals(IntVector)}. Two vectors are considered equal if both
     * vectors are the same {@link IntVector#size() size} and all corresponding pairs of elements in the two vectors
     * are {@link IntComparisons#eq(int, int)}.
     *
     * @param aVector The LHS of the equality test
     * @param bVector The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final IntVector aVector, @NotNull final IntVector bVector) {
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
        try (final CloseablePrimitiveIteratorOfInt aIterator = aVector.iterator();
                final CloseablePrimitiveIteratorOfInt bIterator = bVector.iterator()) {
            while (aIterator.hasNext()) {
                if (!IntComparisons.eq(aIterator.nextInt(), bIterator.nextInt())) {
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
     * Helper method for implementing {@link IntVector#hashCode()}. An iterative equivalent of
     * {@code IntComparisons.hashCode(vector.toArray())}.
     *
     * @param vector The IntVector to hash
     * @return The hash code
     * @see IntComparisons#hashCode(int[])
     */
    static int hashCode(@NotNull final IntVector vector) {
        int result = 1;
        if (vector.isEmpty()) {
            return result;
        }
        try (final CloseablePrimitiveIteratorOfInt iterator = vector.iterator()) {
            while (iterator.hasNext()) {
                result = 31 * result + IntComparisons.hashCode(iterator.nextInt());
            }
        }
        return result;
    }

    /**
     * Base class for all "indirect" IntVector implementations.
     */
    abstract class Indirect implements IntVector {

        @Override
        public int[] toArray() {
            return copyToArray();
        }

        @Override
        public int[] copyToArray() {
            final int size = intSize("IntVector.copyToArray");
            final int[] result = new int[size];
            try (final CloseablePrimitiveIteratorOfInt iterator = iterator()) {
                for (int ei = 0; ei < size; ++ei) {
                    result[ei] = iterator.nextInt();
                }
            }
            return result;
        }

        @Override
        public IntVector getDirect() {
            return new IntVectorDirect(copyToArray());
        }

        @Override
        public final String toString() {
            return IntVector.toString(this, 10);
        }

        @Override
        public final boolean equals(Object other) {
            return other instanceof IntVector && IntVector.equals(this, (IntVector) other);
        }

        @Override
        public final boolean equals(@Nullable IntVector other) {
            return other != null && IntVector.equals(this, other);
        }

        @Override
        public final int hashCode() {
            return IntVector.hashCode(this);
        }

        protected final Object writeReplace() {
            return getDirect();
        }
    }
}
