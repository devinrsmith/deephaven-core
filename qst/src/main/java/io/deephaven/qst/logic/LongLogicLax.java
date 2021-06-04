package io.deephaven.qst.logic;

import io.deephaven.qst.column.type.GenericType;
import org.immutables.value.Value.Immutable;

@Immutable(builder = false, copy = false)
public abstract class LongLogicLax extends LongLogicBase {

    public static LongLogicLax instance() {
        return ImmutableLongLogicLax.of();
    }

    @Override
    public final long transform(boolean x) {
        throw new IllegalArgumentException();
    }

    @Override
    public final long transform(byte x) {
        return x;
    }

    @Override
    public final long transform(char x) {
        return x;
    }

    @Override
    public final long transform(short x) {
        return x;
    }

    @Override
    public final long transform(int x) {
        return x;
    }

    @Override
    public final long transform(float x) {
        return (long)x;
    }

    @Override
    public final long transform(double x) {
        return (long)x;
    }

    @Override
    public final long transform(String x) {
        return Long.parseLong(x);
    }

    @Override
    public final <T> long transform(GenericType<T> type, T value) {
        throw new IllegalArgumentException();
    }
}
