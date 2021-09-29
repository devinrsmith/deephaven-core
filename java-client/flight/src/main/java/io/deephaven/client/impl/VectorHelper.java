package io.deephaven.client.impl;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;

public class VectorHelper {


    /*
    public static IntVector of(BufferAllocator allocator, String name, PrimitiveIterator.OfInt iterator) {
        final IntVector vector = new IntVector(name, allocator);
        vector.allocateNew();
        int i;
        for (i = 0; iterator.hasNext(); ++i) {
            final int value = iterator.nextInt();
            if (value == Integer.MIN_VALUE) {
                vector.setNull(i);
            } else {
                vector.setSafe(i, value);
            }
        }
        vector.setValueCount(i);
        return vector;
    }*/

    public static IntVector of(BufferAllocator allocator, String name, int[] array, int offset, int len) {
        final IntVector vector = new IntVector(name, allocator);
        vector.allocateNew(len);
        for (int i = 0; i < len; i++) {
            final int value = array[offset + i];
            if (value == Integer.MIN_VALUE) {
                vector.setNull(i);
            } else {
                vector.set(i, value);
            }
        }
        vector.setValueCount(len);
        return vector;
    }

    public static BigIntVector of(BufferAllocator allocator, String name, long[] array, int offset, int len) {
        final BigIntVector vector = new BigIntVector(name, allocator);
        vector.allocateNew(len);
        for (int i = 0; i < len; i++) {
            final long value = array[offset + i];
            if (value == Long.MIN_VALUE) {
                vector.setNull(i);
            } else {
                vector.set(i, value);
            }
        }
        vector.setValueCount(len);
        return vector;
    }
}
