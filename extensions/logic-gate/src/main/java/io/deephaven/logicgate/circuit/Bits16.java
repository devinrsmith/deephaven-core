package io.deephaven.logicgate.circuit;

import io.deephaven.logicgate.BitBuilder;
import io.deephaven.qst.table.Table;
import org.immutables.value.Value.Immutable;

import java.time.Duration;
import java.util.Random;

@Immutable
public abstract class Bits16 {

    public static Bits16 of(long x, BitBuilder builder) {
        Bits4 b0 = Bits4.of(x, builder);
        Bits4 b4 = Bits4.of(x >> 4, builder);
        Bits4 b8 = Bits4.of(x >> 8, builder);
        Bits4 b12 = Bits4.of(x >> 12, builder);
        return ImmutableBits16.builder().b0(b0).b4(b4).b8(b8).b12(b12).build();
    }

    public static Bits16 ofTimed(Random r, Duration min, Duration max, BitBuilder builder) {
        Bits4 b0 = Bits4.ofTimed(r, min, max, builder);
        Bits4 b4 = Bits4.ofTimed(r, min, max, builder);
        Bits4 b8 = Bits4.ofTimed(r, min, max, builder);
        Bits4 b12 = Bits4.ofTimed(r, min, max, builder);
        return ImmutableBits16.builder().b0(b0).b4(b4).b8(b8).b12(b12).build();
    }

    public abstract Bits4 b0();

    public abstract Bits4 b4();

    public abstract Bits4 b8();

    public abstract Bits4 b12();

    public final Table merge() {
        return Table.merge(b0().merge(), b4().merge(), b8().merge(), b12().merge());
    }
}
