//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.sink.appender;

import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.Type;

public interface CharAppender extends Appender {

    static CharAppender get(Appender appender) {
        return (CharAppender) appender;
    }

    static void append(CharAppender appender, char value) {
        appender.set(value);
        appender.advance();
    }

    static void appendNull(CharAppender appender) {
        appender.setNull();
        appender.advance();
    }

    @Override
    default CharType type() {
        return Type.charType();
    }

    void setNull();

    void set(char value);

    void advance();
}
