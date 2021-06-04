package io.deephaven.qst.logic;

import io.deephaven.qst.GenericType;
import org.immutables.value.Value.Immutable;

@Immutable(builder = false, copy = false)
public abstract class FloatLogic extends FloatLogicBase {

    public static FloatLogic instance() {
        return ImmutableFloatLogic.of();
    }

    @Override
    public final float transform(boolean x) {
        throw new IllegalArgumentException();
    }

    @Override
    public final float transform(byte x) {
        return x;
    }

    @Override
    public final float transform(char x) {
        return x;
    }

    @Override
    public final float transform(short x) {
        return x;
    }

    @Override
    public final float transform(int x) {
        return x;
    }

    @Override
    public final float transform(long x) {
        return x;
    }

    @Override
    public final float transform(double x) {
        return (float)x;
    }

    @Override
    public final float transform(String x) {
        return Float.parseFloat(x);
    }

    @Override
    public final <T> float transform(GenericType<T> type, T value) {
        throw new IllegalArgumentException();
    }
}
