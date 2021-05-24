package io.deephaven.qst;

import java.util.Arrays;
import org.immutables.value.Value.Immutable;

@Immutable(builder = false)
public abstract class LongType extends ColumnTypeBase<Long> {

    public static LongType instance() {
        return ImmutableLongType.of();
    }

    LongType() {
        super(Arrays.asList(long.class, Long.class));
    }

    @Override
    public final boolean isValidValue(Long item) {
        return item == null || item != Long.MIN_VALUE; // todo QueryConstants
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        return LongType.class.getName();
    }
}
