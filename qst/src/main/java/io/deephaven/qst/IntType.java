package io.deephaven.qst;

import java.util.Arrays;
import org.immutables.value.Value.Immutable;

@Immutable(builder = false)
public abstract class IntType extends ColumnTypeBase<Integer> {

    public static IntType instance() {
        return ImmutableIntType.of();
    }

    IntType() {
        super(Arrays.asList(int.class, Integer.class));
    }

    @Override
    public final boolean isValidValue(Integer item) {
        return item == null || item != Integer.MIN_VALUE; // todo QueryConstants
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        return IntType.class.getName();
    }
}
