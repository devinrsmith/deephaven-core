package io.deephaven.processor.appender;

import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.Type;

public interface FloatAppender extends Appender {
    static FloatAppender get(Appender appender) {
        return (FloatAppender) appender;
    }

    static void append(FloatAppender appender, float value) {
        appender.set(value);
        appender.advance();
    }

    static void appendNull(FloatAppender appender) {
        appender.setNull();
        appender.advance();
    }

    @Override
    default FloatType type() {
        return Type.floatType();
    }

    void setNull();

    void set(float value);

    void advance();

    default ByteAppender asByte() {
        return Utils.floatAsByte(this);
    }

    default ShortAppender asShort() {
        return Utils.floatAsShort(this);
    }
}
