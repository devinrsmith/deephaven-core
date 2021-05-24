package io.deephaven.qst;

import org.immutables.value.Value.Immutable;

@Immutable(builder = false)
public abstract class LongLogicStrict extends LongLogicBase {

    public static LongLogicStrict instance() {
        return ImmutableLongLogicStrict.of();
    }

    @Override
    public final long transform(boolean x) {
        throw new IllegalArgumentException();
    }

    @Override
    public final long transform(int x) {
        return x;
    }

    @Override
    public final long transform(double x) {
        throw new IllegalArgumentException();
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
