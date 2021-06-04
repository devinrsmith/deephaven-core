package io.deephaven.qst.logic;

import io.deephaven.qst.column.type.GenericType;
import org.immutables.value.Value.Immutable;

@Immutable(builder = false, copy = false)
public abstract class ByteLogicLax extends ByteLogicBase {

    public static ByteLogicLax instance() {
        return ImmutableByteLogicLax.of();
    }

    @Override
    public final byte transform(boolean x) {
        throw new IllegalArgumentException();
    }

    @Override
    public final byte transform(char x) {
        if ((byte)x != x) {
            throw new IllegalArgumentException();
        }
        return (byte)x;
    }

    @Override
    public final byte transform(short x) {
        if ((byte)x != x) {
            throw new IllegalArgumentException();
        }
        return (byte)x;
    }

    @Override
    public final byte transform(int x) {
        if ((byte)x != x) {
            throw new IllegalArgumentException();
        }
        return (byte)x;
    }

    @Override
    public final byte transform(long x) {
        if ((byte)x != x) {
            throw new IllegalArgumentException();
        }
        return (byte)x;
    }

    @Override
    public final byte transform(float x) {
        if ((byte)x != x) {
            throw new IllegalArgumentException();
        }
        return (byte)x;
    }

    @Override
    public final byte transform(double x) {
        if ((byte)x != x) {
            throw new IllegalArgumentException();
        }
        return (byte)x;
    }

    @Override
    public final byte transform(String x) {
        return Byte.parseByte(x);
    }

    @Override
    public final <T> byte transform(GenericType<T> type, T value) {
        throw new IllegalArgumentException();
    }
}
