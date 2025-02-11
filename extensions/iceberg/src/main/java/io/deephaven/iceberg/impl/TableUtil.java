//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.impl;

import org.apache.iceberg.Table;

import java.util.Optional;
import java.util.UUID;

public final class TableUtil {
    public static Optional<UUID> uuid(Table table) {
        try {
            return Optional.of(table.uuid());
        } catch (RuntimeException e) {
            // The UUID method is unsupported for v1 Iceberg tables since uuid is optional for v1 tables.
            return Optional.empty();
        }
    }
}
