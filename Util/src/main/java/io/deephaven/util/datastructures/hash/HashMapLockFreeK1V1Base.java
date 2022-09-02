/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.datastructures.hash;

import gnu.trove.iterator.TLongLongIterator;

public abstract class HashMapLockFreeK1V1Base extends HashMapK1V1 {
    private volatile long[] keysAndValues;

    public HashMapLockFreeK1V1Base(int desiredInitialCapacity, float loadFactor, long noEntryValue) {
        super(desiredInitialCapacity, loadFactor, noEntryValue);
        this.keysAndValues = null;
    }

    @Override
    protected void setKeysAndValues(long[] keysAndValues) {
        this.keysAndValues = keysAndValues;
    }

    @Override
    public final long put(long key, long value) {
        return putImpl(keysAndValues, key, value, false);
    }

    @Override
    public final long putIfAbsent(long key, long value) {
        return putImpl(keysAndValues, key, value, true);
    }

    @Override
    public final long get(long key) {
        return getImpl(keysAndValues, key);
    }

    @Override
    public final long remove(long key) {
        return removeImpl(keysAndValues, key);
    }

    public final int capacity() {
        return capacityImpl(keysAndValues);
    }

    @Override
    public final void clear() {
        clearImpl(keysAndValues);
    }

    public final void resetToNull() {
        resetToNullImpl();
        keysAndValues = null;
    }

    @Override
    public final long[] keys() {
        return keysOrValuesImpl(keysAndValues, null, false);
    }

    @Override
    public final long[] keys(long[] array) {
        return keysOrValuesImpl(keysAndValues, array, false);
    }

    @Override
    public final long[] values() {
        return keysOrValuesImpl(keysAndValues, null, true);
    }

    @Override
    public final long[] values(long[] array) {
        return keysOrValuesImpl(keysAndValues, array, true);
    }

    @Override
    public final TLongLongIterator iterator() {
        return iteratorImpl(keysAndValues);
    }
}
