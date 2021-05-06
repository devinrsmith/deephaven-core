package io.deephaven.logicgate.circuit;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.logicgate.BitBuilder;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class Bits4 {

    public static Bits4 of(long x, BitBuilder builder) {
        Table b0 = (x & 1) != 0 ? builder.one() : builder.zero();
        Table b1 = (x & 2) != 0 ? builder.one() : builder.zero();
        Table b2 = (x & 4) != 0 ? builder.one() : builder.zero();
        Table b3 = (x & 8) != 0 ? builder.one() : builder.zero();
        return ImmutableBits4.builder().b0(b0).b1(b1).b2(b2).b3(b3).build();
    }

    public abstract Table b0();

    public abstract Table b1();

    public abstract Table b2();

    public abstract Table b3();

    public final Table merge() {
        return TableTools.merge(b0(), b1(), b2(), b3());
    }
}
