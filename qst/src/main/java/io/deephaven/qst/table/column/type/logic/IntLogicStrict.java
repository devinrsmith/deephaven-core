package io.deephaven.qst.table.column.type.logic;

import io.deephaven.qst.table.column.type.GenericType;
import org.immutables.value.Value.Immutable;

@Immutable(builder = false, copy = false)
public abstract class IntLogicStrict extends IntLogicBase {

    public static IntLogicStrict instance() {
        return ImmutableIntLogicStrict.of();
    }

    @Override
    public final int transform(boolean x) {
        throw new IllegalArgumentException();
    }

    @Override
    public final int transform(byte x) {
        return x;
    }

    @Override
    public final int transform(char x) {
        return x;
    }

    @Override
    public final int transform(short x) {
        return x;
    }

    @Override
    public final int transform(long x) {
        try {
            return Math.toIntExact(x);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public final int transform(float x) {
        throw new IllegalArgumentException();
    }

    @Override
    public final int transform(double x) {
        throw new IllegalArgumentException();
    }

    @Override
    public final int transform(String x) {
        return Integer.parseInt(x);
    }

    @Override
    public final <T> int transform(GenericType<T> type, T value) {
        throw new IllegalArgumentException();
    }
}
