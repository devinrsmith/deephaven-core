package io.deephaven.qst;

import java.util.Arrays;
import java.util.Objects;
import org.immutables.value.Value.Immutable;

@Immutable(builder = false)
public abstract class DoubleType extends ColumnTypeBase<Double> {

    public static DoubleType instance() {
        return ImmutableDoubleType.of();
    }

    DoubleType() {
        super(Arrays.asList(double.class, Double.class));
    }

    @Override
    public boolean isValidValue(Double item) {
        return item == null || item != -Double.MAX_VALUE; // todo QueryConstants
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        return DoubleType.class.getName();
    }
}
