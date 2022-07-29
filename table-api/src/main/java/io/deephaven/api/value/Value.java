/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.value;

import io.deephaven.api.ColumnName;
import io.deephaven.api.expression.Expression;

/**
 * Represents a value.
 *
 * @see Expression
 */
public interface Value extends Expression {
    static Value of(long value) {
        return ValueLong.of(value);
    }

    <T> T walk(Visitor<T> visitor);

    interface Visitor<T> {
        // TODO (deephaven-core#831): Add more table api Value structuring

        T visit(ColumnName x);

        T visit(long x);
    }
}
