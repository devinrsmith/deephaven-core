//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;

/**
 * Setter, does not advance.
 */
@FunctionalInterface
public interface RecordConsumer extends Consumer<ConsumerRecord<?, ?>> {

    @Override
    @NotNull
    default RecordConsumer andThen(@NotNull Consumer<? super ConsumerRecord<?, ?>> after) {
        return Consumer.super.andThen(after)::accept;
    }
}
