//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.sink.appender;

import io.deephaven.qst.type.GenericType;

public interface ObjectAppender<T> extends Appender {

    static <X> ObjectAppender<X> get(Appender appender, GenericType<X> type) {
        final ObjectAppender<?> objAppender = (ObjectAppender<?>) appender;
        if (!objAppender.type().equals(type)) {
            throw new ClassCastException(
                    String.format("Types not equal, type=%s, actual=%s", type, objAppender.type()));
        }
        // noinspection unchecked
        return (ObjectAppender<X>) objAppender;
    }

    static <X> void append(ObjectAppender<? super X> appender, X value) {
        appender.set(value);
        appender.advance();
    }

    static void appendNull(ObjectAppender<?> appender) {
        appender.setNull();
        appender.advance();
    }

    @Override
    GenericType<T> type();

    void setNull();

    void set(T value);

    void advance();

    default <T2 extends T> ObjectAppender<T2> asType(GenericType<T2> type) {
        return Utils.objectAsType(this, type);
    }
}
