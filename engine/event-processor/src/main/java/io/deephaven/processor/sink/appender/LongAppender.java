//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.sink.appender;

import io.deephaven.processor.factory.EventProcessorStreamSpec.Key;
import io.deephaven.processor.sink.Stream;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.Type;

public interface LongAppender extends Appender {

    static LongAppender get(Appender appender) {
        return (LongAppender) appender;
    }

    static LongAppender get(Stream stream, Key<Long> key) {
        return get(Stream.get(stream, key));
    }

    static LongAppender getIfPresent(Stream stream, Key<Long> key) {
        return get(Stream.getIfPresent(stream, key));
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
