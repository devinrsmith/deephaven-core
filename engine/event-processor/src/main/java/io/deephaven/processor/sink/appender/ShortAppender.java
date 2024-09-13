//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.sink.appender;

import io.deephaven.processor.sink.Key;
import io.deephaven.processor.sink.Stream;
import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.Type;

public interface ShortAppender extends Appender {

    static ShortAppender get(Appender appender) {
        return (ShortAppender) appender;
    }

    static ShortAppender get(Stream stream, Key<Short> key) {
        return get(Stream.get(stream, key));
    }

    static ShortAppender getIfPresent(Stream stream, Key<Short> key) {
        return get(Stream.getIfPresent(stream, key));
    }

    static void append(ShortAppender appender, short value) {
        appender.set(value);
        appender.advance();
    }

    static void appendNull(ShortAppender appender) {
        appender.setNull();
        appender.advance();
    }

    @Override
    default ShortType type() {
        return Type.shortType();
    }

    void setNull();

    void set(short value);

    void advance();

    default ByteAppender asByte() {
        return Utils.shortAsByte(this);
    }
}
