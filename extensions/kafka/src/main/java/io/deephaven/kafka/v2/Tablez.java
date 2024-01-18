/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;

public final class Tablez {

    public static Table of(TableOptions<?, ?> options) {
        return options.table();
    }

    public static PartitionedTable ofPartitioned(TableOptions<?, ?> options) {
        return options.partitionedTable();
    }
}
