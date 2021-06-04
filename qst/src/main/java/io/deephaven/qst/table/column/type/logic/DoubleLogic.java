package io.deephaven.qst.table.column.type.logic;

import io.deephaven.qst.table.column.type.GenericType;
import org.immutables.value.Value.Immutable;

@Immutable(builder = false, copy = false)
public abstract class DoubleLogic extends DoubleLogicBase {

    public static DoubleLogic instance() {
        return ImmutableDoubleLogic.of();
    }

    @Override
    public final double transform(boolean x) {
        throw new IllegalArgumentException();
    }

    @Override
    public final double transform(byte x) {
        return x;
    }

    @Override
    public final double transform(char x) {
        return x;
    }

    @Override
    public final double transform(short x) {
        return x;
    }

    @Override
    public final double transform(int x) {
        return x;
    }

    @Override
    public final double transform(long x) {
        return x;
    }

    @Override
    public final double transform(float x) {
        return x;
    }

    @Override
    public final double transform(String x) {
        return Double.parseDouble(x);
    }

    @Override
    public final <T> double transform(GenericType<T> type, T value) {
        throw new IllegalArgumentException();
    }
}
