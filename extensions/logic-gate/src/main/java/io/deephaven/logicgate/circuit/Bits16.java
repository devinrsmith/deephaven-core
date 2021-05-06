package io.deephaven.logicgate.circuit;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.logicgate.BitBuilder;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class Bits16 {

    public static Bits16 of(long x, BitBuilder builder) {
        Bits4 b0 = Bits4.of(x, builder);
        Bits4 b4 = Bits4.of(x >> 4, builder);
        Bits4 b8 = Bits4.of(x >> 8, builder);
        Bits4 b12 = Bits4.of(x >> 12, builder);
        return ImmutableBits16.builder().b0(b0).b4(b4).b8(b8).b12(b12).build();
    }

    public abstract Bits4 b0();

    public abstract Bits4 b4();

    public abstract Bits4 b8();

    public abstract Bits4 b12();

    public final Table merge() {
        return TableTools.merge(b0().merge(), b4().merge(), b8().merge(), b12().merge());
    }
}
