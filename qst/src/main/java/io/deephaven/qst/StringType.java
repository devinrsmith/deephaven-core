package io.deephaven.qst;

import java.util.Collections;

public final class StringType extends ColumnTypeBase<String> {

    private static final StringType INSTANCE = new StringType();

    public static StringType instance() {
        return INSTANCE;
    }

    private StringType() {
        super(Collections.singletonList(String.class));
    }

    @Override
    public final boolean isValidValue(String item) {
        return true;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        return StringType.class.getName();
    }
}
