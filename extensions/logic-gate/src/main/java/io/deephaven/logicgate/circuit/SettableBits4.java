package io.deephaven.logicgate.circuit;

import io.deephaven.logicgate.BitBuilder;
import io.deephaven.logicgate.SettableBit;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class SettableBits4 {

    public static SettableBits4 create(BitBuilder builder) {
        return ImmutableSettableBits4.builder().s0(builder.settable()).s1(builder.settable())
            .s2(builder.settable()).s3(builder.settable()).build();
    }

    public abstract SettableBit s0();

    public abstract SettableBit s1();

    public abstract SettableBit s2();

    public abstract SettableBit s3();

    public final void set() {
        s0().set();
        s1().set();
        s2().set();
        s3().set();
    }

    public final void clear() {
        s0().clear();
        s1().clear();
        s2().clear();
        s3().clear();
    }

    public final void set(byte b) {
        if ((b & 1) != 0) {
            s0().set();
        } else {
            s0().clear();
        }
        if ((b & 2) != 0) {
            s1().set();
        } else {
            s1().clear();
        }
        if ((b & 4) != 0) {
            s2().set();
        } else {
            s2().clear();
        }
        if ((b & 8) != 0) {
            s3().set();
        } else {
            s3().clear();
        }
    }

    public final Bits4 toBits() {
        return ImmutableBits4.builder().b0(s0().bit()).b1(s1().bit()).b2(s2().bit()).b3(s3().bit())
            .build();
    }
}
