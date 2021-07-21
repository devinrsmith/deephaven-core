package io.deephaven.logicgate.circuit;

import io.deephaven.logicgate.LogicGateBuilder;
import io.deephaven.logicgate.LookupTableNand;
import io.deephaven.qst.table.Table;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class WriteQst {
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

    public static Table adder4(long x, long y) {
        LogicGateBuilder b = LookupTableNand.create();
        FullAdder1BitBuilderImpl adder1 =
                ImmutableFullAdder1BitBuilderImpl.builder().xor(b).or(b).and(b).build();
        FullAdder4BitBuilderImpl adder4 =
                ImmutableFullAdder4BitBuilderImpl.builder().adder(adder1).build();
        FullAdder4Bit results = adder4.build(Bits4.of(x, b), Bits4.of(y, b), b.zero());
        return results.s().merge();
    }

    private static void write(String path, Table table) throws IOException {
        try (OutputStream out = Files.newOutputStream(Paths.get(path));
            BufferedOutputStream buffOut = new BufferedOutputStream(out);
            ObjectOutputStream oOut = new ObjectOutputStream(buffOut)) {
            oOut.writeObject(table);
        }
    }

    public static void main(String[] args) throws IOException {
        Table table = adder64(42L, 2021L);
        write("42plus2021.qst", table);

        write("8plus3.qst", adder4(8, 3));
    }
}
