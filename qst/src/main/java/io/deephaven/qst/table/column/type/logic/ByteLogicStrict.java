package io.deephaven.qst.table.column.type.logic;

import io.deephaven.qst.table.column.type.GenericType;
import org.immutables.value.Value.Immutable;

@Immutable(builder = false, copy = false)
public abstract class ByteLogicStrict extends ByteLogicBase {

    public static ByteLogicStrict instance() {
        return ImmutableByteLogicStrict.of();
    }

    @Override
    public final byte transform(boolean x) {
        throw new IllegalArgumentException();
    }

    @Override
    public final byte transform(char x) {
        return (byte)x;
    }

    @Override
    public final byte transform(short x) {
        return (byte)x;
    }

    @Override
    public final byte transform(int x) {
        return (byte)x;
    }

    @Override
    public final byte transform(long x) {
        return (byte)x;
    }

    @Override
    public final byte transform(float x) {
        return (byte)x;
    }

    @Override
    public final byte transform(double x) {
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
