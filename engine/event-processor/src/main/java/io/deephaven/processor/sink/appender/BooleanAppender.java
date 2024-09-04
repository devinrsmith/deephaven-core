//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.sink.appender;

import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.Type;

public interface BooleanAppender extends Appender {

    static BooleanAppender get(Appender appender) {
        return (BooleanAppender) appender;
    }

    static void append(BooleanAppender appender, boolean value) {
        appender.set(value);
        appender.advance();
    }

    static void appendNull(BooleanAppender appender) {
        appender.setNull();
        appender.advance();
    }

    @Override
    default BooleanType type() {
        return Type.booleanType();
    }

    void setNull();

    void set(boolean value);

    void advance();
}
