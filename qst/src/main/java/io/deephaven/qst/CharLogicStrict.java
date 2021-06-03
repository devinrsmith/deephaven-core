package io.deephaven.qst;

import org.immutables.value.Value.Immutable;

@Immutable(builder = false, copy = false)
public abstract class CharLogicStrict extends CharLogicBase {

    public static CharLogicStrict instance() {
        return ImmutableCharLogicStrict.of();
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
        if ((char)x != x) {
            throw new IllegalArgumentException();
        }
        return (char)x;
    }

    @Override
    public final char transform(long x) {
        if ((char)x != x) {
            throw new IllegalArgumentException();
        }
        return (char)x;
    }

    @Override
    public final char transform(float x) {
        if ((char)x != x) {
            throw new IllegalArgumentException();
        }
        return (char)x;
    }

    @Override
    public final char transform(double x) {
        if ((char)x != x) {
            throw new IllegalArgumentException();
        }
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
