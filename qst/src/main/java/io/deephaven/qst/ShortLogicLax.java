package io.deephaven.qst;

import org.immutables.value.Value.Immutable;

@Immutable(builder = false, copy = false)
public abstract class ShortLogicLax extends ShortLogicBase {

    public static ShortLogicLax instance() {
        return ImmutableShortLogicLax.of();
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
        return (short)x;
    }

    @Override
    public final short transform(int x) {
        return (short)x;
    }

    @Override
    public final short transform(long x) {
        return (short)x;
    }

    @Override
    public final short transform(float x) {
        return (short)x;
    }

    @Override
    public final short transform(double x) {
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
