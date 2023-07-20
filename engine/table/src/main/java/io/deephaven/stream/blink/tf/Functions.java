package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.BoxedDoubleType;
import io.deephaven.qst.type.BoxedLongType;

class Functions {

    enum PrimitiveBoolean implements BooleanFunction<Object> {
        INSTANCE;

        @Override
        public boolean applyAsBoolean(Object value) {
            return (boolean) value;
        }
    }

    enum PrimitiveChar implements CharFunction<Object> {
        INSTANCE;

        @Override
        public char applyAsChar(Object value) {
            return (char) value;
        }
    }

    enum PrimitiveByte implements ByteFunction<Object> {
        INSTANCE;

        @Override
        public byte applyAsByte(Object value) {
            return (byte) value;
        }
    }

    enum PrimitiveShort implements ShortFunction<Object> {
        INSTANCE;

        @Override
        public short applyAsShort(Object value) {
            return (short) value;
        }
    }

    enum PrimitiveInt implements IntFunction<Object> {
        INSTANCE;

        @Override
        public int applyAsInt(Object value) {
            return (int) value;
        }
    }

    enum PrimitiveLong implements LongFunction<Object> {
        INSTANCE;

        @Override
        public long applyAsLong(Object value) {
            return (long) value;
        }
    }

    enum PrimitiveFloat implements FloatFunction<Object> {
        INSTANCE;

        @Override
        public float applyAsFloat(Object value) {
            return (float) value;
        }
    }

    enum PrimitiveDouble implements DoubleFunction<Object> {
        INSTANCE;

        @Override
        public double applyAsDouble(Object value) {
            return (double) value;
        }
    }

    enum BoxedLong implements ObjectFunction<Object, Long> {
        INSTANCE;

        @Override
        public BoxedLongType returnType() {
            return BoxedLongType.of();
        }

        @Override
        public Long apply(Object value) {
            return (Long) value;
        }
    }

    enum BoxedDouble implements ObjectFunction<Object, Double> {
        INSTANCE;

        @Override
        public BoxedDoubleType returnType() {
            return BoxedDoubleType.of();
        }

        @Override
        public Double apply(Object value) {
            return (Double) value;
        }
    }
}
