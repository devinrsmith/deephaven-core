package io.deephaven.stream.blink.tf;

import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;

import java.time.Instant;

// todo make internal?
public class MapToPrimitives {

    public static <T> ByteFunction<T> mapBoolean(BooleanFunction<T> f) {
        return x -> BooleanUtils.booleanAsByte(f.applyAsBoolean(x));
    }

    public static <T> ByteFunction<T> mapBoolean(ObjectFunction<T, Boolean> f) {
        return f.mapByte(BooleanUtils::booleanAsByte);
    }

    public static <T> ByteFunction<T> mapByte(ObjectFunction<T, Byte> f) {
        return f.mapByte(TypeUtils::unbox);
    }

    public static <T> CharFunction<T> mapChar(ObjectFunction<T, Character> f) {
        return f.mapChar(TypeUtils::unbox);
    }

    public static <T> ShortFunction<T> mapShort(ObjectFunction<T, Short> f) {
        return f.mapShort(TypeUtils::unbox);
    }

    public static <T> IntFunction<T> mapInt(ObjectFunction<T, Integer> f) {
        return f.mapInt(TypeUtils::unbox);
    }

    public static <T> LongFunction<T> mapLong(ObjectFunction<T, Long> f) {
        return f.mapLong(TypeUtils::unbox);
    }

    public static <T> FloatFunction<T> mapFloat(ObjectFunction<T, Float> f) {
        return f.mapFloat(TypeUtils::unbox);
    }

    public static <T> DoubleFunction<T> mapDouble(ObjectFunction<T, Double> f) {
        return f.mapDouble(TypeUtils::unbox);
    }

    public static <T> LongFunction<T> mapInstant(ObjectFunction<T, Instant> f) {
        return f.mapLong(DateTimeUtils::epochNanos);
    }
}
