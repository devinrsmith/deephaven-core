//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.util.Objects;

/**
 * Setter, does not advance.
 */
@FunctionalInterface
interface HeaderConsumer {

    static HeaderConsumer wrap(RecordConsumer consumer) {
        return (record, header, headerIndex) -> consumer.accept(record);
    }

    void accept(ConsumerRecord<?, ?> record, Header header, int headerIndex);

    default HeaderConsumer andThen(HeaderConsumer after) {
        Objects.requireNonNull(after);
        return (r, h, i) -> {
            accept(r, h, i);
            after.accept(r, h, i);
        };
    }
}
