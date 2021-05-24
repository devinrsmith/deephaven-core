package io.deephaven.qst;

import org.immutables.value.Value.Immutable;

@Immutable(builder = false)
public abstract class BooleanLogicStrict extends BooleanLogicBase {

    public static BooleanLogicStrict instance() {
        return ImmutableBooleanLogicStrict.of();
    }

    @Override
    public final boolean transform(int x) {
        throw new IllegalArgumentException();
    }

    @Override
    public final boolean transform(long x) {
        throw new IllegalArgumentException();
    }

    @Override
    public final boolean transform(double x) {
        throw new IllegalArgumentException();
    }

    @Override
    public final boolean transform(String x) {
        if ("true".equals(x)) {
            return true;
        }
        if ("false".equals(x)) {
            return false;
        }
        throw new IllegalArgumentException();
    }

    @Override
    public final <T> boolean transform(GenericType<T> type, T value) {
        throw new IllegalArgumentException();
    }
}
