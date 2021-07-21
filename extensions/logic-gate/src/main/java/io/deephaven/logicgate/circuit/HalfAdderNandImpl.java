package io.deephaven.logicgate.circuit;

import io.deephaven.qst.table.Table;
import io.deephaven.logicgate.NandBuilder;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class HalfAdderNandImpl implements HalfAdderBuilder {

    public abstract NandBuilder nand();

    @Override
    public final HalfAdder build(Table a, Table b) {
        Table nand4 = nand().nand(a, b);
        Table nand1 = nand().nand(a, nand4);
        Table nand2 = nand().nand(nand4, b);
        Table nand3 = nand().nand(nand1, nand2);
        Table nand5 = nand().nand(nand4, nand4);
        return ImmutableHalfAdder.builder().a(a).b(b).c(nand5).s(nand3).build();
    }
}
