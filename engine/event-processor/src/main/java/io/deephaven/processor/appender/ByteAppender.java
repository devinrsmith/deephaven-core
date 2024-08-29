package io.deephaven.processor.appender;

import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.Type;

public interface ByteAppender extends Appender {

    static ByteAppender get(Appender appender) {
        return (ByteAppender) appender;
    }

    static void append(ByteAppender appender, byte value) {
        appender.set(value);
        appender.advance();
    }

    static void appendNull(ByteAppender appender) {
        appender.setNull();
        appender.advance();
    }

    @Override
    default ByteType type() {
        return Type.byteType();
    }

    void setNull();

    void set(byte value);

    void advance();
}
