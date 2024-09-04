//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.sink.appender;

import io.deephaven.qst.type.GenericType;

import java.util.Objects;

final class Utils {

    public static ByteAppender shortAsByte(ShortAppender appender) {
        return new ShortAsByte(appender);
    }

    public static ByteAppender intAsByte(IntAppender appender) {
        return new IntAsByte(appender);
    }

    public static ShortAppender intAsShort(IntAppender appender) {
        return new IntAsShort(appender);
    }

    public static ByteAppender longAsByte(LongAppender appender) {
        return new LongAsByte(appender);
    }

    public static ShortAppender longAsShort(LongAppender appender) {
        return new LongAsShort(appender);
    }

    public static IntAppender longAsInt(LongAppender appender) {
        return new LongAsInt(appender);
    }

    public static ByteAppender floatAsByte(FloatAppender appender) {
        return new FloatAsByte(appender);
    }

    public static ShortAppender floatAsShort(FloatAppender appender) {
        return new FloatAsShort(appender);
    }

    public static ByteAppender doubleAsByte(DoubleAppender appender) {
        return new DoubleAsByte(appender);
    }

    public static ShortAppender doubleAsShort(DoubleAppender appender) {
        return new DoubleAsShort(appender);
    }

    public static IntAppender doubleAsInt(DoubleAppender appender) {
        return new DoubleAsInt(appender);
    }

    public static FloatAppender doubleAsFloat(DoubleAppender appender) {
        return new DoubleAsFloat(appender);
    }

    public static <T1, T2 extends T1> ObjectAppender<T2> objectAsType(ObjectAppender<T1> appender,
            GenericType<T2> type) {
        return new ObjectAsType<>(appender, type);
    }

    private static class ShortAsByte implements ByteAppender {
        private final ShortAppender appender;

        public ShortAsByte(ShortAppender appender) {
            this.appender = Objects.requireNonNull(appender);
        }

        @Override
        public void setNull() {
            appender.setNull();
        }

        @Override
        public void set(byte value) {
            appender.set(value);
        }

        @Override
        public void advance() {
            appender.advance();
        }
    }

    private static class IntAsByte implements ByteAppender {
        private final IntAppender appender;

        public IntAsByte(IntAppender appender) {
            this.appender = Objects.requireNonNull(appender);
        }

        @Override
        public void setNull() {
            appender.setNull();
        }

        @Override
        public void set(byte value) {
            appender.set(value);
        }

        @Override
        public void advance() {
            appender.advance();
        }
    }

    private static class IntAsShort implements ShortAppender {
        private final IntAppender appender;

        public IntAsShort(IntAppender appender) {
            this.appender = Objects.requireNonNull(appender);
        }

        @Override
        public void setNull() {
            appender.setNull();
        }

        @Override
        public void set(short value) {
            appender.set(value);
        }

        @Override
        public void advance() {
            appender.advance();
        }

        @Override
        public ByteAppender asByte() {
            return intAsByte(appender);
        }
    }

    private static class LongAsByte implements ByteAppender {
        private final LongAppender appender;

        public LongAsByte(LongAppender appender) {
            this.appender = Objects.requireNonNull(appender);
        }

        @Override
        public void setNull() {
            appender.setNull();
        }

        @Override
        public void set(byte value) {
            appender.set(value);
        }

        @Override
        public void advance() {
            appender.advance();
        }
    }

    private static class LongAsShort implements ShortAppender {
        private final LongAppender appender;

        public LongAsShort(LongAppender appender) {
            this.appender = Objects.requireNonNull(appender);
        }

        @Override
        public void setNull() {
            appender.setNull();
        }

        @Override
        public void set(short value) {
            appender.set(value);
        }

        @Override
        public void advance() {
            appender.advance();
        }

        @Override
        public ByteAppender asByte() {
            return longAsByte(appender);
        }
    }

    private static class LongAsInt implements IntAppender {
        private final LongAppender appender;

        public LongAsInt(LongAppender appender) {
            this.appender = Objects.requireNonNull(appender);
        }

        @Override
        public void setNull() {
            appender.setNull();
        }

        @Override
        public void set(int value) {
            appender.set(value);
        }

        @Override
        public void advance() {
            appender.advance();
        }

        @Override
        public ByteAppender asByte() {
            return longAsByte(appender);
        }

        @Override
        public ShortAppender asShort() {
            return longAsShort(appender);
        }
    }

    private static class FloatAsByte implements ByteAppender {
        private final FloatAppender appender;

        public FloatAsByte(FloatAppender appender) {
            this.appender = Objects.requireNonNull(appender);
        }

        @Override
        public void setNull() {
            appender.setNull();
        }

        @Override
        public void set(byte value) {
            appender.set(value);
        }

        @Override
        public void advance() {
            appender.advance();
        }
    }

    private static class FloatAsShort implements ShortAppender {
        private final FloatAppender appender;

        public FloatAsShort(FloatAppender appender) {
            this.appender = Objects.requireNonNull(appender);
        }

        @Override
        public void setNull() {
            appender.setNull();
        }

        @Override
        public void set(short value) {
            appender.set(value);
        }

        @Override
        public void advance() {
            appender.advance();
        }

        @Override
        public ByteAppender asByte() {
            return floatAsByte(appender);
        }
    }

    private static class DoubleAsByte implements ByteAppender {
        private final DoubleAppender appender;

        public DoubleAsByte(DoubleAppender appender) {
            this.appender = Objects.requireNonNull(appender);
        }

        @Override
        public void setNull() {
            appender.setNull();
        }

        @Override
        public void set(byte value) {
            appender.set(value);
        }

        @Override
        public void advance() {
            appender.advance();
        }
    }

    private static class DoubleAsShort implements ShortAppender {
        private final DoubleAppender appender;

        public DoubleAsShort(DoubleAppender appender) {
            this.appender = Objects.requireNonNull(appender);
        }

        @Override
        public void setNull() {
            appender.setNull();
        }

        @Override
        public void set(short value) {
            appender.set(value);
        }

        @Override
        public void advance() {
            appender.advance();
        }

        @Override
        public ByteAppender asByte() {
            return doubleAsByte(appender);
        }
    }

    private static class DoubleAsInt implements IntAppender {
        private final DoubleAppender appender;

        public DoubleAsInt(DoubleAppender appender) {
            this.appender = Objects.requireNonNull(appender);
        }

        @Override
        public void setNull() {
            appender.setNull();
        }

        @Override
        public void set(int value) {
            appender.set(value);
        }

        @Override
        public void advance() {
            appender.advance();
        }

        @Override
        public ByteAppender asByte() {
            return doubleAsByte(appender);
        }

        @Override
        public ShortAppender asShort() {
            return doubleAsShort(appender);
        }
    }

    private static class DoubleAsFloat implements FloatAppender {
        private final DoubleAppender appender;

        public DoubleAsFloat(DoubleAppender appender) {
            this.appender = Objects.requireNonNull(appender);
        }

        @Override
        public void setNull() {
            appender.setNull();
        }

        @Override
        public void set(float value) {
            appender.set(value);
        }

        @Override
        public void advance() {
            appender.advance();
        }

        @Override
        public ByteAppender asByte() {
            return doubleAsByte(appender);
        }

        @Override
        public ShortAppender asShort() {
            return doubleAsShort(appender);
        }
    }

    private static class ObjectAsType<T1, T2 extends T1> extends ObjectAppenderBase<T2> {

        private final ObjectAppender<T1> appender;

        private ObjectAsType(ObjectAppender<T1> appender, GenericType<T2> type) {
            super(type);
            this.appender = Objects.requireNonNull(appender);
        }

        @Override
        public void setNull() {
            appender.setNull();
        }

        @Override
        public void set(T2 value) {
            appender.set(value);
        }

        @Override
        public void advance() {
            appender.advance();
        }

        @Override
        public <T3 extends T2> ObjectAppender<T3> asType(GenericType<T3> type) {
            return objectAsType(appender, type);
        }
    }
}
