/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;
import java.util.Map;

public class KafkaToolsNew {

    public static Table blinkTable(TableOptions<?, ?> options) {
        return options.table();
    }

    static void safeCloseClient(Throwable t, KafkaConsumer<?, ?> client) {
        try {
            client.close();
        } catch (Throwable t2) {
            t.addSuppressed(t2);
        }
    }
}
