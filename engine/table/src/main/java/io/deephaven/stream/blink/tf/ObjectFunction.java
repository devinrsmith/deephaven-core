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

    static <R> ObjectFunction<R, R> identity(GenericType<R> type) {
        return ObjectFunction.of(Function.identity(), type);
    }

    static <T, R> ObjectFunction<T, R> cast(TypedFunction<T> f) {
        return (ObjectFunction<T, R>) f;
    }

    /**
     * Creates the function composition {@code g ∘ f}.
     *
     * <p>
     * Equivalent to {@code x -> g.apply(f.apply(x))}.
     *
     * @param f the inner function
     * @param g the outer function
     * @return the object function
     * @param <T> the input type
     * @param <R> the intermediate type
     */
    static <T, R, Z> ObjectFunction<T, Z> map(Function<T, R> f, ObjectFunction<R, Z> g) {
        return new ObjectMap<>(f, g);
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

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Equivalent to {@code x -> g.applyAsBoolean(this.apply(x))}.
     *
     * @param g the outer function
     * @return the boolean function
     */
    default BooleanFunction<T> mapBoolean(BooleanFunction<R> g) {
        return BooleanFunction.map(this::apply, g);
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Equivalent to {@code x -> g.applyAsChar(this.apply(x))}.
     *
     * @param g the outer function
     * @return the char function
     */
    default CharFunction<T> mapChar(CharFunction<R> g) {
        return CharFunction.map(this::apply, g);
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Equivalent to {@code x -> g.applyAsByte(this.apply(x))}.
     *
     * @param g the outer function
     * @return the byte function
     */
    default ByteFunction<T> mapByte(ByteFunction<R> g) {
        return ByteFunction.map(this::apply, g);
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Equivalent to {@code x -> g.applyAsShort(this.apply(x))}.
     *
     * @param g the outer function
     * @return the short function
     */
    default ShortFunction<T> mapShort(ShortFunction<R> g) {
        return ShortFunction.map(this::apply, g);
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Equivalent to {@code x -> g.applyAsInt(this.apply(x))}.
     *
     * @param g the outer function
     * @return the int function
     */
    default IntFunction<T> mapInt(IntFunction<R> g) {
        return IntFunction.map(this::apply, g);
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Equivalent to {@code x -> g.applyAsLong(this.apply(x))}.
     *
     * @param g the outer function
     * @return the long function
     */
    default LongFunction<T> mapLong(LongFunction<R> g) {
        return LongFunction.map(this::apply, g);
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Equivalent to {@code x -> g.applyAsFloat(this.apply(x))}.
     *
     * @param g the outer function
     * @return the float function
     */
    default FloatFunction<T> mapFloat(FloatFunction<R> g) {
        return FloatFunction.map(this::apply, g);
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Equivalent to {@code x -> g.applyAsDouble(this.apply(x))}.
     *
     * @param g the outer function
     * @return the double function
     */
    default DoubleFunction<T> mapDouble(DoubleFunction<R> g) {
        return DoubleFunction.map(this::apply, g);
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Equivalent to {@code x -> g.apply(this.apply(x))}.
     *
     * @param g the outer function
     * @return the object function
     */
    default <R2> ObjectFunction<T, R2> mapObj(ObjectFunction<R, R2> g) {
        return ObjectFunction.map(this::apply, g);
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Equivalent to {@code x -> g.apply(this.apply(x))}.
     *
     * @param g the outer function
     * @return the object function
     */
    default <R2> ObjectFunction<T, R2> mapObj(Function<R, R2> g, GenericType<R2> returnType) {
        return ObjectFunction.map(this::apply, ObjectFunction.of(g, returnType));
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Prefer to call one of the more strongly-typed map methods if you have a more specific function type.
     *
     * @param g the outer function
     * @return the function
     * @see #mapBoolean(BooleanFunction)
     * @see #mapChar(CharFunction)
     * @see #mapByte(ByteFunction)
     * @see #mapShort(ShortFunction)
     * @see #mapInt(IntFunction)
     * @see #mapLong(LongFunction)
     * @see #mapFloat(FloatFunction)
     * @see #mapDouble(DoubleFunction)
     */
    default PrimitiveFunction<T> mapPrimitive(PrimitiveFunction<R> g) {
        return MapPrimitiveVisitor.of(this, g);
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Prefer to call one of the more strongly-typed map methods if you have a more specific function type.
     *
     * @param g the outer function
     * @return the function
     * @see #mapPrimitive(PrimitiveFunction)
     * @see #mapObj(ObjectFunction)
     * @see #mapBoolean(BooleanFunction)
     * @see #mapChar(CharFunction)
     * @see #mapByte(ByteFunction)
     * @see #mapShort(ShortFunction)
     * @see #mapInt(IntFunction)
     * @see #mapLong(LongFunction)
     * @see #mapFloat(FloatFunction)
     * @see #mapDouble(DoubleFunction)
     */
    default TypedFunction<T> map(TypedFunction<R> g) {
        return MapVisitor.of(this, g);
    }
}
