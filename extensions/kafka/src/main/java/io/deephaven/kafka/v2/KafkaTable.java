/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.engine.table.Table;

public final class KafkaTable {

    public static Table of(KafkaTableOptions<?, ?> options) {
        return options.table();
    }
}
