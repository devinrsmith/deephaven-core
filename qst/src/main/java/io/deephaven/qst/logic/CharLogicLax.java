package io.deephaven.qst.logic;

import io.deephaven.qst.GenericType;
import org.immutables.value.Value.Immutable;

@Immutable(builder = false, copy = false)
public abstract class CharLogicLax extends CharLogicBase {

    public static CharLogicLax instance() {
        return ImmutableCharLogicLax.of();
    }

    @Override
    public final char transform(boolean x) {
        throw new IllegalArgumentException();
    }

    @Override
    public final char transform(byte x) {
        return (char)x;
    }

    @Override
    public final char transform(short x) {
        return (char)x;
    }

    @Override
    public final char transform(int x) {
        return (char)x;
    }

    @Override
    public final char transform(long x) {
        return (char)x;
    }

    @Override
    public final char transform(float x) {
        return (char)x;
    }

    @Override
    public final char transform(double x) {
        return (char)x;
    }

    @Override
    public final char transform(String x) {
        throw new IllegalArgumentException();
    }

    @Override
    public final <T> char transform(GenericType<T> type, T value) {
        throw new IllegalArgumentException();
    }
}
