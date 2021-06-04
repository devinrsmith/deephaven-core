package io.deephaven.qst.logic;

import io.deephaven.qst.column.GenericType;
import org.immutables.value.Value.Immutable;

@Immutable(builder = false, copy = false)
public abstract class BooleanLogicLax extends BooleanLogicBase {

    public static BooleanLogicLax instance() {
        return ImmutableBooleanLogicLax.of();
    }


    @Override
    public final boolean transform(byte x) {
        return x != 0;
    }

    @Override
    public final boolean transform(char x) {
        return x != 0;
    }

    @Override
    public final boolean transform(short x) {
        return x != 0;
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
    public final boolean transform(float x) {
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
