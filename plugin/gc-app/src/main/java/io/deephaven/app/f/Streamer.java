package io.deephaven.app.f;

import io.deephaven.qst.column.header.ColumnHeader;

public interface Streamer<T> {


    static <T> void of(String x, LongMapp<T> m) {
        ColumnHeader.ofLong("x");
        m.applyAsLong(null);
    }

    void add(T value);

    void failure(Throwable t);
}
