/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.expression;

import io.deephaven.api.RawString;
import io.deephaven.api.Selectable;
import io.deephaven.api.value.Value;

import java.io.Serializable;

/**
 * Represents an evaluate-able expression structure.
 *
 * @see Selectable
 */
public interface Expression extends Serializable {

    <T> T walk(Visitor<T> visitor);

    interface Visitor<T> {
        // TODO (deephaven-core#830): Add more table api Expression structuring

        T visit(Value value);

        T visit(RawString rawString);
    }
}
