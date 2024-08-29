package io.deephaven.processor.appender;

import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.Type;

public interface IntAppender extends Appender {

    static IntAppender get(Appender appender) {
        return (IntAppender) appender;
    }

    static void append(IntAppender appender, int value) {
        appender.set(value);
        appender.advance();
    }

    static void appendNull(IntAppender appender) {
        appender.setNull();
        appender.advance();
    }

    @Override
    default IntType type() {
        return Type.intType();
    }

    void setNull();

    void set(int value);

    void advance();

    default ByteAppender asByte() {
        return Utils.intAsByte(this);
    }

    default ShortAppender asShort() {
        return Utils.intAsShort(this);
    }
}
