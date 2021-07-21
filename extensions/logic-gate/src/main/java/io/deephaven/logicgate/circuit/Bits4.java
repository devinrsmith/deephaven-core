package io.deephaven.logicgate.circuit;

import io.deephaven.logicgate.BitBuilder;
import io.deephaven.qst.table.Table;
import org.immutables.value.Value.Immutable;

import java.time.Duration;
import java.util.Random;

@Immutable
public abstract class Bits4 {

    public static Bits4 of(long x, BitBuilder builder) {
        Table b0 = (x & 1) != 0 ? builder.one() : builder.zero();
        Table b1 = (x & 2) != 0 ? builder.one() : builder.zero();
        Table b2 = (x & 4) != 0 ? builder.one() : builder.zero();
        Table b3 = (x & 8) != 0 ? builder.one() : builder.zero();
        return ImmutableBits4.builder().b0(b0).b1(b1).b2(b2).b3(b3).build();
    }

    public static Bits4 ofTimed(Random r, Duration min, Duration max, BitBuilder builder) {
        Duration diff = max.minus(min);
        // todo: better ways to get true random dist
        Table b0 = builder
            .timedBit(Duration.ofNanos(Math.floorMod(r.nextLong(), diff.toNanos())).plus(min));
        Table b1 = builder
            .timedBit(Duration.ofNanos(Math.floorMod(r.nextLong(), diff.toNanos())).plus(min));
        Table b2 = builder
            .timedBit(Duration.ofNanos(Math.floorMod(r.nextLong(), diff.toNanos())).plus(min));
        Table b3 = builder
            .timedBit(Duration.ofNanos(Math.floorMod(r.nextLong(), diff.toNanos())).plus(min));
        return ImmutableBits4.builder().b0(b0).b1(b1).b2(b2).b3(b3).build();
    }

    public abstract Table b0();

    public abstract Table b1();

    public abstract Table b2();

    public abstract Table b3();

    public final Table merge() {
        return Table.merge(b0(), b1(), b2(), b3());
    }
}
