package io.deephaven.processor.appender;

import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.Type;

public interface DoubleAppender extends Appender {
    static DoubleAppender get(Appender appender) {
        return (DoubleAppender) appender;
    }

    static void append(DoubleAppender appender, double value) {
        appender.set(value);
        appender.advance();
    }

    static void appendNull(DoubleAppender appender) {
        appender.setNull();
        appender.advance();
    }

    @Override
    default DoubleType type() {
        return Type.doubleType();
    }

    void setNull();

    void set(double value);

    void advance();

    default ByteAppender asByte() {
        return Utils.doubleAsByte(this);
    }

    default ShortAppender asShort() {
        return Utils.doubleAsShort(this);
    }

    default IntAppender asInt() {
        return Utils.doubleAsInt(this);
    }

    default FloatAppender asFloat() {
        return Utils.doubleAsFloat(this);
    }
}
