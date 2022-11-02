/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.table;

import java.io.Serializable;

/**
 * The time provider for a {@link TimeTable}.
 *
 * @see TimeProviderSystem
 */
public interface TimeProvider extends Serializable {

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(TimeProviderSystem system);
    }
}