package io.deephaven.stream.blink;

import java.util.Collection;

public interface Producer<T> {
    void add(T value);

    void addAll(Collection<? extends T> values);

    void failure(Throwable t);
}
