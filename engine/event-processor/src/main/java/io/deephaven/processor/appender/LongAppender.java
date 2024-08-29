package io.deephaven.processor.appender;

import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.Type;

public interface LongAppender extends Appender {

    static LongAppender get(Appender appender) {
        return (LongAppender) appender;
    }

    static void append(LongAppender appender, long value) {
        appender.set(value);
        appender.advance();
    }

    static void appendNull(LongAppender appender) {
        appender.setNull();
        appender.advance();
    }

    @Override
    default LongType type() {
        return Type.longType();
    }

    void setNull();

    void set(long value);

    void advance();

    default ByteAppender asByte() {
        return Utils.longAsByte(this);
    }

    default ShortAppender asShort() {
        return Utils.longAsShort(this);
    }

    default IntAppender asInt() {
        return Utils.longAsInt(this);
    }
}
