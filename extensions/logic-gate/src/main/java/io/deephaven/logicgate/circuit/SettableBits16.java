package io.deephaven.logicgate.circuit;

import io.deephaven.logicgate.BitBuilder;
import io.deephaven.logicgate.SettableBit;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class SettableBits16 {

    public static SettableBits16 create(BitBuilder builder) {
        return ImmutableSettableBits16.builder()
            .s0(SettableBits4.create(builder))
            .s4(SettableBits4.create(builder))
            .s8(SettableBits4.create(builder))
            .s12(SettableBits4.create(builder))
            .build();
    }

    public abstract SettableBits4 s0();

    public abstract SettableBits4 s4();

    public abstract SettableBits4 s8();

    public abstract SettableBits4 s12();

    public final void set() {
        s0().set();
        s4().set();
        s8().set();
        s12().set();
    }

    public final void clear() {
        s0().clear();
        s4().clear();
        s8().clear();
        s12().clear();
    }

    public final void set(short b) {
        s0().set((byte)(b & 0xF));
        s4().set((byte)((b >> 4) & 0xF));
        s8().set((byte)((b >> 8) & 0xF));
        s12().set((byte)((b >> 12) & 0xF));
    }

    public final Bits16 toBits() {
        return ImmutableBits16.builder().b0(s0().toBits()).b4(s4().toBits()).b8(s8().toBits()).b12(s12().toBits()).build();
    }
}
