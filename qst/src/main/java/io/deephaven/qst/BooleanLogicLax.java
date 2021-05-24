package io.deephaven.qst;

import org.immutables.value.Value.Immutable;

@Immutable(builder = false)
public abstract class BooleanLogicLax extends BooleanLogicBase {

    public static BooleanLogicLax instance() {
        return ImmutableBooleanLogicLax.of();
    }

    @Override
    public final boolean transform(int x) {
        return x != 0;
    }

    @Override
    public final boolean transform(long x) {
        return x != 0;
    }

    @Override
    public final boolean transform(double x) {
        return x != 0;
    }

    @Override
    public final boolean transform(String x) {
        if ("true".equalsIgnoreCase(x)) {
            return true;
        }
        if ("false".equalsIgnoreCase(x)) {
            return false;
        }
        throw new IllegalArgumentException();
    }

    @Override
    public final <T> boolean transform(GenericType<T> type, T value) {
        throw new IllegalArgumentException();
    }
}
