package io.deephaven.app.f;

import io.deephaven.qst.column.header.ColumnHeader;

import java.time.Instant;

public interface MyB<T> {

    static <T> MyB<T> builder() {
        return null;
    }

    MyB<T> addChar(String name, CharMapp<T> f);

    MyB<T> addByte(String name, ByteMapp<T> f);

    MyB<T> addShort(String name, ShortMapp<T> f);

    MyB<T> addInt(String name, IntMapp<T> f);
    MyB<T> addLong(String name, LongMapp<T> f);

    MyB<T> addFloat(String name, FloatMapp<T> f);

    MyB<T> addDouble(String name, DoubleMapp<T> f);

    MyB<T> addString(String name, ObjectMapp<T, String> f);

    MyB<T> addInstant(String name, ObjectMapp<T, Instant> f);

    <R> MyB<T> add(ColumnHeader<R> header, Mapp<T> f);

    Streamer<T> build();
}
