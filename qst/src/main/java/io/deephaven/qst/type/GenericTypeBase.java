/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

public abstract class GenericTypeBase<T> extends ColumnTypeBase<T> implements GenericType<T> {

    @Override
    public final <V extends Type.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final NativeArrayType<T[], T> arrayType() {
        // noinspection unchecked
        return (NativeArrayType<T[], T>) NativeArrayType.toArrayType(this);
    }
}
