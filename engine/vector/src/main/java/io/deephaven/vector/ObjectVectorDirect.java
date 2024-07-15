//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.vector;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.util.compare.ObjectComparisons;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * An {@link ObjectVector} backed by an array.
 */
public final class ObjectVectorDirect<COMPONENT_TYPE> implements ObjectVector<COMPONENT_TYPE> {

    private static final long serialVersionUID = 9111886364211462917L;

    public static final ObjectVector<?> ZERO_LENGTH_VECTOR = new ObjectVectorDirect<>();

    public static <COMPONENT_TYPE> ObjectVector<COMPONENT_TYPE> empty() {
        // noinspection unchecked
        return (ObjectVector<COMPONENT_TYPE>) ZERO_LENGTH_VECTOR;
    }

    private final COMPONENT_TYPE[] data;
    private final Class<COMPONENT_TYPE> componentType;

    @SuppressWarnings("unchecked")
    public ObjectVectorDirect(@NotNull final COMPONENT_TYPE... data) {
        this.data = Require.neqNull(data, "data");
        componentType = (Class<COMPONENT_TYPE>) data.getClass().getComponentType();
    }

    @Override
    public COMPONENT_TYPE get(final long index) {
        if (index < 0 || index >= data.length) {
            return null;
        }
        return data[(int) index];
    }

    @Override
    public ObjectVector<COMPONENT_TYPE> subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return new ObjectVectorSlice<>(this, fromIndexInclusive, toIndexExclusive - fromIndexInclusive);
    }

    @Override
    public ObjectVector<COMPONENT_TYPE> subVectorByPositions(final long[] positions) {
        return new ObjectSubVector<>(this, positions);
    }

    @Override
    public COMPONENT_TYPE[] toArray() {
        return data;
    }

    @Override
    public COMPONENT_TYPE[] copyToArray() {
        return Arrays.copyOf(data, data.length);
    }

    public COMPONENT_TYPE[] copyOfRange(int from, int to) {
        return Arrays.copyOfRange(data, from, to);
    }

    @Override
    public CloseableIterator<COMPONENT_TYPE> iterator(final long fromIndexInclusive, final long toIndexExclusive) {
        if (fromIndexInclusive == 0 && toIndexExclusive == data.length) {
            return CloseableIterator.of(data);
        }
        return ObjectVector.super.iterator(fromIndexInclusive, toIndexExclusive);
    }

    @Override
    public long size() {
        return data.length;
    }

    @Override
    public Class<COMPONENT_TYPE> getComponentType() {
        return componentType;
    }

    @Override
    public ObjectVectorDirect<COMPONENT_TYPE> getDirect() {
        return this;
    }

    @Override
    public String toString() {
        return ObjectVector.toString(this, 10);
    }

    @Override
    public boolean equals(final Object obj) {
        // noinspection unchecked
        return obj instanceof ObjectVector && equals((ObjectVector<COMPONENT_TYPE>) obj);
    }

    @Override
    public boolean equals(@Nullable ObjectVector<COMPONENT_TYPE> other) {
        if (other == null) {
            return false;
        }
        return other instanceof ObjectVectorDirect
                ? ObjectComparisons.eq(data, ((ObjectVectorDirect<COMPONENT_TYPE>) other).data)
                : ObjectVector.equals(this, other);
    }

    @Override
    public int hashCode() {
        return ObjectComparisons.hashCode(data);
    }
}
