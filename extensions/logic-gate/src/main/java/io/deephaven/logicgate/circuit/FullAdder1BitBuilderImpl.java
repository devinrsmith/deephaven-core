package io.deephaven.logicgate.circuit;

import io.deephaven.qst.table.Table;
import io.deephaven.logicgate.AndBuilder;
import io.deephaven.logicgate.OrBuilder;
import io.deephaven.logicgate.XorBuilder;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class FullAdder1BitBuilderImpl implements FullAdder1BitBuilder {

    public static <T extends XorBuilder & AndBuilder & OrBuilder> FullAdder1BitBuilder create(
        T logic) {
        return ImmutableFullAdder1BitBuilderImpl.builder().xor(logic).and(logic).or(logic).build();
    }

    public abstract XorBuilder xor();

    public abstract AndBuilder and();

    public abstract OrBuilder or();

    @Override
    public final FullAdder1Bit build(Table a, Table b, Table cIn) {
        Table aXorB = xor().xor(a, b);
        Table S = xor().xor(aXorB, cIn);

        Table t = and().and(aXorB, cIn);
        Table aAndB = and().and(a, b);
        Table cOut = or().or(t, aAndB);

        return ImmutableFullAdder1Bit.builder().a(a).b(b).cIn(cIn).cOut(cOut).s(S).build();
    }
}
