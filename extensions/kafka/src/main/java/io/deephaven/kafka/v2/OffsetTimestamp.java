/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.time.Duration;
import java.time.Instant;

@Immutable
@SimpleStyle
abstract class OffsetTimestamp implements Offset {

    private static final Duration ROUND_UP_MILLI = Duration.ofMillis(1).minusNanos(1);

    @Parameter
    public abstract Instant since();

    final long kafkaSafeEpochMillis() {
        // We need to make sure the caller doesn't see any timestamps before #since. Given that kafka is limited to
        // millisecond precision, we need to round up if we aren't on an exact milli boundary.
        // For example, if the user requests timestamp "2020-01-01T01:02:03.456000001Z", we can't just use #toEpochMilli
        // since that will just drop the sub-milli precision.
        // org.apache.kafka.clients.consumer.KafkaConsumer#offsetsForTimes
        return since().plus(ROUND_UP_MILLI).toEpochMilli();
    }
}
