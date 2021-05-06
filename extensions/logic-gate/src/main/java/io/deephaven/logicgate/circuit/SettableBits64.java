package io.deephaven.logicgate.circuit;

import io.deephaven.logicgate.BitBuilder;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class SettableBits64 {

    public static SettableBits64 create(BitBuilder builder) {
        return ImmutableSettableBits64.builder()
            .s0(SettableBits16.create(builder))
            .s16(SettableBits16.create(builder))
            .s32(SettableBits16.create(builder))
            .s48(SettableBits16.create(builder))
            .build();
    }

    public abstract SettableBits16 s0();

    public abstract SettableBits16 s16();

    public abstract SettableBits16 s32();

    public abstract SettableBits16 s48();

    public final void set() {
        s0().set();
        s16().set();
        s32().set();
        s48().set();
    }

    public final void clear() {
        s0().clear();
        s16().clear();
        s32().clear();
        s48().clear();
    }

    public final void set(long b) {
        s0().set((short)(b & 0xFFFF));
        s16().set((short)((b >> 16) & 0xFFFF));
        s32().set((short)((b >> 32) & 0xFFFF));
        s48().set((short)((b >> 48) & 0xFFFF));
    }

    public final Bits64 toBits() {
        return ImmutableBits64.builder()
            .b0(s0().toBits())
            .b16(s16().toBits())
            .b32(s32().toBits())
            .b48(s48().toBits())
            .build();
    }
}
