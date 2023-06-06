package io.deephaven.app;

import io.deephaven.qst.array.ArrayBuilder;

public interface Mapper<Obj, T, Builder extends ArrayBuilder<T, ?, ?>> {
    void add(Builder builder, Obj obj);
}
