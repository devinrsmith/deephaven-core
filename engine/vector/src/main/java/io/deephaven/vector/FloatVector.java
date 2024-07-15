//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVector and run "./gradlew replicateVectors" to regenerate
//
// @formatter:off
package io.deephaven.vector;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfFloat;
import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.compare.FloatComparisons;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A {@link Vector} of primitive floats.
 */
public interface FloatVector extends Vector<FloatVector>, Iterable<Float> {

    long serialVersionUID = -1373264425081841175L;

    static PrimitiveVectorType<FloatVector, Float> type() {
        return PrimitiveVectorType.of(FloatVector.class, FloatType.of());
    }

    /**
     * Get the element of this FloatVector at offset {@code index}. If {@code index} is not within range
     * {@code [0, size())}, will return the {@link QueryConstants#NULL_FLOAT null float}.
     *
     * @param index An offset into this FloatVector
     * @return The element at the specified offset, or the {@link QueryConstants#NULL_FLOAT null float}
     */
    float get(long index);

    @Override
    FloatVector subVector(long fromIndexInclusive, long toIndexExclusive);

    @Override
    FloatVector subVectorByPositions(long[] positions);

    @Override
    float[] toArray();

    @Override
    float[] copyToArray();

    @Override
    FloatVector getDirect();

    /**
     * Logically equivalent to {@code other != null && FloatComparisons.eq(toArray(), other.toArray())}.
     *
     * @param other the other vector
     * @return {@code true} if the two vectors are equal
     * @see FloatComparisons#eq(float[], float[])
     */
    @Override
    boolean equals(@Nullable FloatVector other);

    /**
     * Logically equivalent to {@code FloatComparisons.hashCode(toArray())}.
     *
     * @return the hash code value for this vector
     * @see FloatComparisons#hashCode(float[])
     */
    @Override
    int hashCode();

    @Override
    @FinalDefault
    default CloseablePrimitiveIteratorOfFloat iterator() {
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
    default CloseablePrimitiveIteratorOfFloat iterator(final long fromIndexInclusive, final long toIndexExclusive) {
        Require.leq(fromIndexInclusive, "fromIndexInclusive", toIndexExclusive, "toIndexExclusive");
        return new CloseablePrimitiveIteratorOfFloat() {

            long nextIndex = fromIndexInclusive;

            @Override
            public float nextFloat() {
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
        return float.class;
    }

    @Override
    @FinalDefault
    default String toString(final int prefixLength) {
        return toString(this, prefixLength);
    }

    static String floatValToString(final Object val) {
        return val == null ? NULL_ELEMENT_STRING : primitiveFloatValToString((Float) val);
    }

    static String primitiveFloatValToString(final float val) {
        return val == QueryConstants.NULL_FLOAT ? NULL_ELEMENT_STRING : Float.toString(val);
    }

    /**
     * Helper method for implementing {@link Object#toString()}.
     *
     * @param vector The FloatVector to convert to a String
     * @param prefixLength The maximum prefix of {@code vector} to convert
     * @return The String representation of {@code vector}
     */
    static String toString(@NotNull final FloatVector vector, final int prefixLength) {
        if (vector.isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = (int) Math.min(vector.size(), prefixLength);
        try (final CloseablePrimitiveIteratorOfFloat iterator = vector.iterator(0, displaySize)) {
            builder.append(primitiveFloatValToString(iterator.nextFloat()));
            iterator.forEachRemaining(
                    (final float value) -> builder.append(',').append(primitiveFloatValToString(value)));
        }
        if (displaySize == vector.size()) {
            builder.append(']');
        } else {
            builder.append(", ...]");
        }
        return builder.toString();
    }

    /**
     * Helper method for implementing {@link FloatVector#equals(FloatVector)}. Two vectors are considered equal if both
     * vectors are the same {@link FloatVector#size() size} and all corresponding pairs of elements in the two vectors
     * are {@link FloatComparisons#eq(float, float)}.
     *
     * @param aVector The LHS of the equality test
     * @param bVector The RHS of the equality test
     * @return Whether the two inputs are equal
     */
    static boolean equals(@NotNull final FloatVector aVector, @NotNull final FloatVector bVector) {
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
        try (final CloseablePrimitiveIteratorOfFloat aIterator = aVector.iterator();
                final CloseablePrimitiveIteratorOfFloat bIterator = bVector.iterator()) {
            while (aIterator.hasNext()) {
                if (!FloatComparisons.eq(aIterator.nextFloat(), bIterator.nextFloat())) {
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
     * Helper method for implementing {@link FloatVector#hashCode()}. An iterative equivalent of
     * {@code FloatComparisons.hashCode(vector.toArray())}.
     *
     * @param vector The FloatVector to hash
     * @return The hash code
     * @see FloatComparisons#hashCode(float[])
     */
    static int hashCode(@NotNull final FloatVector vector) {
        int result = 1;
        if (vector.isEmpty()) {
            return result;
        }
        try (final CloseablePrimitiveIteratorOfFloat iterator = vector.iterator()) {
            while (iterator.hasNext()) {
                result = 31 * result + FloatComparisons.hashCode(iterator.nextFloat());
            }
        }
        return result;
    }

    /**
     * Base class for all "indirect" FloatVector implementations.
     */
    abstract class Indirect implements FloatVector {

        @Override
        public float[] toArray() {
            return copyToArray();
        }

        @Override
        public float[] copyToArray() {
            final int size = intSize("FloatVector.copyToArray");
            final float[] result = new float[size];
            try (final CloseablePrimitiveIteratorOfFloat iterator = iterator()) {
                for (int ei = 0; ei < size; ++ei) {
                    result[ei] = iterator.nextFloat();
                }
            }
            return result;
        }

        @Override
        public FloatVector getDirect() {
            return new FloatVectorDirect(copyToArray());
        }

        @Override
        public final String toString() {
            return FloatVector.toString(this, 10);
        }

        @Override
        public final boolean equals(Object other) {
            return other instanceof FloatVector && FloatVector.equals(this, (FloatVector) other);
        }

        @Override
        public final boolean equals(@Nullable FloatVector other) {
            return other != null && FloatVector.equals(this, other);
        }

        @Override
        public final int hashCode() {
            return FloatVector.hashCode(this);
        }

        protected final Object writeReplace() {
            return getDirect();
        }
    }
}
