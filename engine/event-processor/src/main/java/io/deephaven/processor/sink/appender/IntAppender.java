//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.sink.appender;

import io.deephaven.processor.factory.EventProcessorStreamSpec.Key;
import io.deephaven.processor.sink.Stream;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.Type;

public interface IntAppender extends Appender {

    static IntAppender get(Appender appender) {
        return (IntAppender) appender;
    }

    static IntAppender get(Stream stream, Key<Integer> key) {
        return get(Stream.get(stream, key));
    }

    static IntAppender getIfPresent(Stream stream, Key<Integer> key) {
        return get(Stream.getIfPresent(stream, key));
    }

    static void append(IntAppender appender, int value) {
        appender.set(value);
        appender.advance();
    }

    static void appendNull(IntAppender appender) {
        appender.setNull();
        appender.advance();
    }

    // THEY Want this to be chunk oriented

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
