/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

enum OffsetImpl implements OffsetInternal {
    BEGINNING, END, COMMITTED
}
