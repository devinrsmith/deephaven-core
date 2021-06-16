package io.deephaven.qst.table.agg;

import io.deephaven.qst.table.JoinMatch;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class Array implements Aggregation {

    public static Array of(JoinMatch match) {
        return ImmutableArray.of(match);
    }

    public static Array of(String x) {
        return of(JoinMatch.parse(x));
    }

    @Parameter
    public abstract JoinMatch match();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
