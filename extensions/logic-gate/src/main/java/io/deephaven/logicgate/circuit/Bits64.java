package io.deephaven.logicgate.circuit;

import io.deephaven.qst.table.Table;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.logicgate.BitBuilder;
import java.time.Duration;
import java.util.Random;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class Bits64 {

    public static Bits64 zero(BitBuilder builder) {
        return of(0, builder);
    }

    public static Bits64 one(BitBuilder builder) {
        return of(1, builder);
    }

    public static Bits64 of(long x, BitBuilder builder) {
        Bits16 b0 = Bits16.of(x, builder);
        Bits16 b16 = Bits16.of(x >> 16, builder);
        Bits16 b32 = Bits16.of(x >> 32, builder);
        Bits16 b48 = Bits16.of(x >> 48, builder);
        return ImmutableBits64.builder().b0(b0).b16(b16).b32(b32).b48(b48).build();
    }

    public static Bits64 ofTimed(Random r, Duration min, Duration max, BitBuilder builder) {
        Bits16 b0 = Bits16.ofTimed(r, min, max, builder);
        Bits16 b16 = Bits16.ofTimed(r, min, max, builder);
        Bits16 b32 = Bits16.ofTimed(r, min, max, builder);
        Bits16 b48 = Bits16.ofTimed(r, min, max, builder);
        return ImmutableBits64.builder().b0(b0).b16(b16).b32(b32).b48(b48).build();
    }

    public abstract Bits16 b0();

    public abstract Bits16 b16();

    public abstract Bits16 b32();

    public abstract Bits16 b48();

    public final Table merge() {
        return Table.merge(b0().merge(), b16().merge(), b32().merge(), b48().merge());
    }
}
