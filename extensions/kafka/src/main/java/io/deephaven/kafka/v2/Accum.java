/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

public interface Accum<T> {

    void accumulate(Iterable<T> src);

    void accumulate(T[] src, int offset, int len);

    void flush();
}
