/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;

public final class KafkaTable {

    public static TableDefinition definition(KafkaTableOptions<?, ?> options) {
        return options.tableDefinition();
    }

    public static Table of(KafkaTableOptions<?, ?> options) {
        return options.table();
    }

    // todo: generic entry point for non-table based use-cases
}
