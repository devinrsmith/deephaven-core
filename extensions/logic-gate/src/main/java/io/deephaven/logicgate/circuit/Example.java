package io.deephaven.logicgate.circuit;

import io.deephaven.db.tables.Table;
import io.deephaven.logicgate.LogicGateBuilder;
import io.deephaven.logicgate.LookupTableNand;

public class Example {

    public static Table adder4(long x, long y) {
        LogicGateBuilder b = LookupTableNand.create();
        FullAdder1BitBuilderImpl adder1 =
            ImmutableFullAdder1BitBuilderImpl.builder().xor(b).or(b).and(b).build();
        FullAdder4BitBuilderImpl adder4 =
            ImmutableFullAdder4BitBuilderImpl.builder().adder(adder1).build();
        FullAdder4Bit results = adder4.build(Bits4.of(x, b), Bits4.of(y, b), b.zero());
        return results.s().merge();
    }

    public static Table adder16(long x, long y) {
        LogicGateBuilder b = LookupTableNand.create();
        FullAdder1BitBuilderImpl adder1 =
            ImmutableFullAdder1BitBuilderImpl.builder().xor(b).or(b).and(b).build();
        FullAdder4BitBuilderImpl adder4 =
            ImmutableFullAdder4BitBuilderImpl.builder().adder(adder1).build();
        FullAdder16BitBuilderImpl adder16 =
            ImmutableFullAdder16BitBuilderImpl.builder().adder(adder4).build();
        FullAdder16Bit results = adder16.build(Bits16.of(x, b), Bits16.of(y, b), b.zero());
        return results.s().merge();
    }

    public static Table adder64(long x, long y) {
        LogicGateBuilder b = LookupTableNand.create();
        FullAdder1BitBuilderImpl adder1 =
            ImmutableFullAdder1BitBuilderImpl.builder().xor(b).or(b).and(b).build();
        FullAdder4BitBuilderImpl adder4 =
            ImmutableFullAdder4BitBuilderImpl.builder().adder(adder1).build();
        FullAdder16BitBuilderImpl adder16 =
            ImmutableFullAdder16BitBuilderImpl.builder().adder(adder4).build();
        FullAdder64BitBuilderImpl adder64 =
            ImmutableFullAdder64BitBuilderImpl.builder().adder(adder16).build();
        FullAdder64Bit results = adder64.build(Bits64.of(x, b), Bits64.of(y, b), b.zero());
        return results.s().merge();
    }

    public static Bits64 zero() {
        return Bits64.zero(LookupTableNand.create());
    }

    public static Bits64 one() {
        return Bits64.one(LookupTableNand.create());
    }

    public static Bits64 next(Bits64 x) {
        LogicGateBuilder b = LookupTableNand.create();
        FullAdder1BitBuilderImpl adder1 =
            ImmutableFullAdder1BitBuilderImpl.builder().xor(b).or(b).and(b).build();
        FullAdder4BitBuilderImpl adder4 =
            ImmutableFullAdder4BitBuilderImpl.builder().adder(adder1).build();
        FullAdder16BitBuilderImpl adder16 =
            ImmutableFullAdder16BitBuilderImpl.builder().adder(adder4).build();
        FullAdder64BitBuilderImpl adder64 =
            ImmutableFullAdder64BitBuilderImpl.builder().adder(adder16).build();
        FullAdder64Bit results = adder64.build(x, Bits64.one(b), b.zero());
        return results.s();
    }

}
