package io.deephaven.chunk.util.hashing;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

final class ArrayComparator<T> implements Comparator<T[]> {
    private final Comparator<T> comparator;

    public ArrayComparator(Comparator<T> comparator) {
        this.comparator = Objects.requireNonNull(comparator);
    }

    @Override
    public int compare(T[] o1, T[] o2) {
        return Arrays.compare(o1, o2, comparator);
    }
}
