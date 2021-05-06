package io.deephaven.logicgate.circuit;

import io.deephaven.db.tables.Table;
import io.deephaven.logicgate.AndBuilder;
import io.deephaven.logicgate.XorBuilder;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class HalfAdderImpl implements HalfAdderBuilder {

    public abstract XorBuilder xor();

    public abstract AndBuilder and();

    @Override
    public final HalfAdder build(Table a, Table b) {
        return ImmutableHalfAdder.builder().a(a).b(b).c(and().and(a, b)).s(xor().xor(a, b)).build();
    }
}
