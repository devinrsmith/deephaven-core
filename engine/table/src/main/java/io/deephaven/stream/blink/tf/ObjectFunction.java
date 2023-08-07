package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.BoxedBooleanType;
import io.deephaven.qst.type.BoxedByteType;
import io.deephaven.qst.type.BoxedCharType;
import io.deephaven.qst.type.BoxedFloatType;
import io.deephaven.qst.type.BoxedIntType;
import io.deephaven.qst.type.BoxedLongType;
import io.deephaven.qst.type.BoxedShortType;
import io.deephaven.qst.type.GenericType;

import java.util.function.Function;

public interface ObjectFunction<T, R> extends TypedFunction<T> {

    static <T, R> ObjectFunction<T, R> of(Function<T, R> f, GenericType<R> returnType) {
        return new ObjectFunctionImpl<>(f, returnType);
    }

    static <T, R> ObjectFunction<T, R> cast(GenericType<R> expected) {
        return ObjectFunction.of(t -> expected.clazz().cast(t), expected);
    }

    static <T, R> ObjectFunction<T, R> ofSingle(R sentinel, GenericType<R> type) {
        return new ObjectFunctionImpl<>(m -> sentinel, type);
    }

    static <R> ObjectFunction<R, R> identity(GenericType<R> type) {
        return ObjectFunction.of(Function.identity(), type);
    }

    static <T> ObjectFunction<T, Boolean> booleanObject() {
        return ObjectFunction.of(x -> (Boolean) x, BoxedBooleanType.of());
    }

    static <T> ObjectFunction<T, Character> charObject() {
        return ObjectFunction.of(x -> (Character) x, BoxedCharType.of());
    }

    static <T> ObjectFunction<T, Byte> byteObject() {
        return ObjectFunction.of(x -> (Byte) x, BoxedByteType.of());
    }

    static <T> ObjectFunction<T, Short> shortObject() {
        return ObjectFunction.of(x -> (Short) x, BoxedShortType.of());
    }

    static <T> ObjectFunction<T, Integer> intObject() {
        return ObjectFunction.of(x -> (Integer) x, BoxedIntType.of());
    }

    static <T> ObjectFunction<T, Long> longObject() {
        return ObjectFunction.of(x -> (Long) x, BoxedLongType.of());
    }

    static <T> ObjectFunction<T, Float> floatObject() {
        return ObjectFunction.of(x -> (Float) x, BoxedFloatType.of());
    }

    /**
     * Equivalent to {@code of(x -> (Double) x, BoxedDoubleType.of())}
     *
     * @return the double object function
     * @param <T> the type
     */
    static <T> ObjectFunction<T, Double> doubleObject() {
        // noinspection unchecked
        return (ObjectFunction<T, Double>) Functions.BoxedDouble.INSTANCE;
    }

    static <T, R> ObjectFunction<T, R> cast(TypedFunction<T> f) {
        return (ObjectFunction<T, R>) f;
    }

    GenericType<R> returnType();

    R apply(T value);

    @Override
    default <Z> Z walk(Visitor<T, Z> visitor) {
        return visitor.visit(this);
    }

    default ObjectFunction<T, R> onNullInput(R onNull) {
        return ObjectFunction.of(x -> x == null ? onNull : apply(x), returnType());
    }

    @Override
    default ObjectFunction<T, R> mapInput(Function<T, T> f) {
        return ObjectFunction.of(x -> apply(f.apply(x)), returnType());
    }

    default <R2> ObjectFunction<T, R2> asChecked(GenericType<R2> type) {
        return mapObj(cast(type));
    }

    default <R2> ObjectFunction<T, R2> as(@SuppressWarnings("unused") GenericType<R2> type) {
        //noinspection unchecked
        return (ObjectFunction<T, R2>) this;
    }

    default BooleanFunction<T> mapBoolean(BooleanFunction<R> f) {
        return value -> f.applyAsBoolean(apply(value));
    }

    default CharFunction<T> mapChar(CharFunction<R> f) {
        return value -> f.applyAsChar(apply(value));
    }

    default ByteFunction<T> mapByte(ByteFunction<R> f) {
        return value -> f.applyAsByte(apply(value));
    }

    default ShortFunction<T> mapShort(ShortFunction<R> f) {
        return value -> f.applyAsShort(apply(value));
    }

    default IntFunction<T> mapInt(IntFunction<R> f) {
        return value -> f.applyAsInt(apply(value));
    }

    default LongFunction<T> mapLong(LongFunction<R> f) {
        return value -> f.applyAsLong(apply(value));
    }

    default FloatFunction<T> mapFloat(FloatFunction<R> f) {
        return value -> f.applyAsFloat(apply(value));
    }

    default DoubleFunction<T> mapDouble(DoubleFunction<R> f) {
        return value -> f.applyAsDouble(apply(value));
    }

    default <R2> ObjectFunction<T, R2> mapObj(Function<R, R2> f, GenericType<R2> returnType) {
        return ObjectFunction.of(t -> f.apply(ObjectFunction.this.apply(t)), returnType);
    }

    default <R2> ObjectFunction<T, R2> mapObj(ObjectFunction<R, R2> f) {
        return ObjectFunction.of(t -> f.apply(ObjectFunction.this.apply(t)), f.returnType());
    }

    default TypedFunction<T> map(TypedFunction<R> f) {
        return MapVisitor.of(this, f);
    }
}
