package io.deephaven.qst.table.column.type.logic;

import io.deephaven.qst.table.column.type.GenericType;
import org.immutables.value.Value.Immutable;

@Immutable(builder = false, copy = false)
public abstract class ShortLogicStrict extends ShortLogicBase {

    public static ShortLogicStrict instance() {
        return ImmutableShortLogicStrict.of();
    }

    @Override
    public final short transform(boolean x) {
        throw new IllegalArgumentException();
    }
    
    @Override
    public final short transform(byte x) {
        return x;
    }

    @Override
    public final short transform(char x) {
        if ((short)x != x) {
            throw new IllegalArgumentException();
        }
        return (short)x;
    }

    @Override
    public final short transform(int x) {
        if ((short)x != x) {
            throw new IllegalArgumentException();
        }
        return (short)x;
    }

    @Override
    public final short transform(long x) {
        if ((short)x != x) {
            throw new IllegalArgumentException();
        }
        return (short)x;
    }

    @Override
    public final short transform(float x) {
        if ((short)x != x) {
            throw new IllegalArgumentException();
        }
        return (short)x;
    }

    @Override
    public final short transform(double x) {
        if ((short)x != x) {
            throw new IllegalArgumentException();
        }
        return (short)x;
    }

    @Override
    public final short transform(String x) {
        return Short.parseShort(x);
    }

    @Override
    public final <T> short transform(GenericType<T> type, T value) {
        throw new IllegalArgumentException();
    }
}
