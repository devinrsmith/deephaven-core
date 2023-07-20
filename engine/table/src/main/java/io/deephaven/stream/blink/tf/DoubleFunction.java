package io.deephaven.stream.blink.tf;

import io.deephaven.qst.type.BoxedDoubleType;
import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.util.function.Function;
import java.util.function.ToDoubleFunction;

import static io.deephaven.stream.blink.tf.MapToPrimitives.mapDouble;

@FunctionalInterface
public interface DoubleFunction<T> extends PrimitiveFunction<T>, ToDoubleFunction<T> {

    /**
     * Assumes the object value is directly castable to a double. Equivalent to {@code x -> (double)x}.
     *
     * @return the double function
     * @param <T> the value type
     */
    static <T> DoubleFunction<T> primitive() {
        //noinspection unchecked
        return (DoubleFunction<T>) Functions.PrimitiveDouble.INSTANCE;
    }

    @Override
    double applyAsDouble(T value);

    @Override
    default DoubleType returnType() {
        return Type.doubleType();
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }

    @Override
    default DoubleFunction<T> mapInput(Function<T, T> f) {
        return x -> applyAsDouble(f.apply(x));
    }

    /**
     * Create a new function which returns {@code onNull} when the value is {@code null}, and otherwise calls {@link #applyAsDouble(Object)}. Equivalent to {@code x -> x == null ? onNull : applyAsDouble(x)}.
     * @param onNull the value to return on null
     * @return the new double function
     */
    default DoubleFunction<T> onNullInput(double onNull) {
        return x -> x == null ? onNull : applyAsDouble(x);
    }
}
