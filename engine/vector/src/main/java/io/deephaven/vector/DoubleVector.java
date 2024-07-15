//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVector and run "./gradlew replicateVectors" to regenerate
//
// @formatter:off
package io.deephaven.vector;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfDouble;
import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.compare.DoubleComparisons;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A {@link Vector} of primitive doubles.
 */
public interface DoubleVector extends Vector<DoubleVector>, Iterable<Double> {

    long serialVersionUID = -1373264425081841175L;

    static PrimitiveVectorType<DoubleVector, Double> type() {
        return PrimitiveVectorType.of(DoubleVector.class, DoubleType.of());
    }

    /**
     * Get the element of this DoubleVector at offset {@code index}. If {@code index} is not within range
     * {@code [0, size())}, will return the {@link QueryConstants#NULL_DOUBLE null double}.
     *
     * @param index An offset into this DoubleVector
     * @return The element at the specified offset, or the {@link QueryConstants#NULL_DOUBLE null double}
     */
    double get(long index);

    @Override
    DoubleVector subVector(long fromIndexInclusive, long toIndexExclusive);

    @Override
    DoubleVector subVectorByPositions(long[] positions);

    @Override
    double[] toArray();

    @Override
    double[] copyToArray();

    @Override
    DoubleVector getDirect();

    /**
     * Logically equivalent to {@code other != null && DoubleComparisons.eq(toArray(), other.toArray())}.
     *
     * @param other the other vector
     * @return {@code true} if the two vectors are equal
     * @see DoubleComparisons#eq(double[], double[])
     */
    @Override
    boolean equals(@Nullable DoubleVector other);

    /**
     * Logically equivalent to {@code DoubleComparisons.hashCode(toArray())}.
     *
     * @return the hash code value for this vector
     * @see DoubleComparisons#hashCode(double[])
     */
    @Override
    int hashCode();

    @Override
    @FinalDefault
    default CloseablePrimitiveIteratorOfDouble iterator() {
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
    default CloseablePrimitiveIteratorOfDouble iterator(final long fromIndexInclusive, final long toIndexExclusive) {
        Require.leq(fromIndexInclusive, "fromIndexInclusive", toIndexExclusive, "toIndexExclusive");
        return new CloseablePrimitiveIteratorOfDouble() {

            long nextIndex = fromIndexInclusive;

            @Override
            public double nextDouble() {
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
        return double.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    static String doubleValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveDoubleValToString((Double) val);
    }

    static String primitiveDoubleValToString(final double val) {
        return val == QueryConstants.NULL_DOUBLE ? NULL_ELEMENT_STRING : Double.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param vector The DoubleVector to convert to a String
     * @param prefixLength The maximum prefix of {@code vector} to convert
     * @return The String representation of {@code vector}
     */
    static String toString(@NotNull final DoubleVector vector, final int prefixLength) {
        if (vector.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(vector.size(), prefixLength);
        try (final CloseablePrimitiveIteratorOfDouble iterator = vector.iterator(0, displaySize)) {
            builder.append(primitiveDoubleValToString(iterator.nextDouble()));
            iterator.forEachRemaining(
                    (final double value) -> builder.append(',').append(primitiveDoubleValToString(value)));
        }
        if (displaySize == vector.size()) {
            builder.append(']');
        } else {
            builder.append(", ...]");
        }
        return builder.toString();
    }

    /**
     * Helper method for implementing {@link DoubleVector#equals(DoubleVector)}. Two vectors are considered equal if both
     * vectors are the same {@link DoubleVector#size() size} and all corresponding pairs of elements in the two vectors
     * are {@link DoubleComparisons#eq(double, double)}.
     *
     * @param aVector The LHS of the equality test
     * @param bVector The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final DoubleVector aVector, @NotNull final DoubleVector bVector) {
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
        try (final CloseablePrimitiveIteratorOfDouble aIterator = aVector.iterator();
                final CloseablePrimitiveIteratorOfDouble bIterator = bVector.iterator()) {
            while (aIterator.hasNext()) {
                if (!DoubleComparisons.eq(aIterator.nextDouble(), bIterator.nextDouble())) {
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
     * Helper method for implementing {@link DoubleVector#hashCode()}. An iterative equivalent of
     * {@code DoubleComparisons.hashCode(vector.toArray())}.
     *
     * @param vector The DoubleVector to hash
     * @return The hash code
     * @see DoubleComparisons#hashCode(double[])
     */
    static int hashCode(@NotNull final DoubleVector vector) {
        int result = 1;
        if (vector.isEmpty()) {
            return result;
        }
        try (final CloseablePrimitiveIteratorOfDouble iterator = vector.iterator()) {
            while (iterator.hasNext()) {
                result = 31 * result + DoubleComparisons.hashCode(iterator.nextDouble());
            }
        }
        return result;
    }

    /**
     * Base class for all "indirect" DoubleVector implementations.
     */
    abstract class Indirect implements DoubleVector {

        @Override
        public double[] toArray() {
            return copyToArray();
        }

        @Override
        public double[] copyToArray() {
            final int size = intSize("DoubleVector.copyToArray");
            final double[] result = new double[size];
            try (final CloseablePrimitiveIteratorOfDouble iterator = iterator()) {
                for (int ei = 0; ei < size; ++ei) {
                    result[ei] = iterator.nextDouble();
                }
            }
            return result;
        }

        @Override
        public DoubleVector getDirect() {
            return new DoubleVectorDirect(copyToArray());
        }

        @Override
        public final String toString() {
            return DoubleVector.toString(this, 10);
        }

        @Override
        public final boolean equals(Object other) {
            return other instanceof DoubleVector && DoubleVector.equals(this, (DoubleVector) other);
        }

        @Override
        public final boolean equals(@Nullable DoubleVector other) {
            return other != null && DoubleVector.equals(this, other);
        }

        @Override
        public final int hashCode() {
            return DoubleVector.hashCode(this);
        }

        protected final Object writeReplace() {
            return getDirect();
        }
    }
}
