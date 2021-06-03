package io.deephaven.qst;

import org.immutables.value.Value.Immutable;

@Immutable(builder = false, copy = false)
public abstract class IntLogicLax extends IntLogicBase {

    public static IntLogicLax instance() {
        return ImmutableIntLogicLax.of();
    }

    @Override
    public final int transform(boolean x) {
        throw new IllegalArgumentException();
    }

    @Override
    public final int transform(long x) {
        return (int)x;
    }

    @Override
    public final int transform(float x) {
        return (int)x;
    }

    @Override
    public final int transform(double x) {
        return (int)x;
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
