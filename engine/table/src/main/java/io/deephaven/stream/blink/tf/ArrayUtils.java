package io.deephaven.stream.blink.tf;

import java.util.Collection;

public class ArrayUtils {

    public static <T> boolean[] toArray(
            BooleanFunction<T> booleanFunction,
            Collection<T> src) {
        final boolean[] out = new boolean[src.size()];
        int i = 0;
        for (T item : src) {
            out[i++] = booleanFunction.applyAsBoolean(item);
        }
        return out;
    }

    public static <T> int[] toArray(
            IntFunction<T> intFunction,
            Collection<T> src) {
        final int[] out = new int[src.size()];
        int i = 0;
        for (T item : src) {
            out[i++] = intFunction.applyAsInt(item);
        }
        return out;
    }
}
