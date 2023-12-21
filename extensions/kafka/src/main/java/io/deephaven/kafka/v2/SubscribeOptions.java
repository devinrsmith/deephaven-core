/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.annotations.BuildableStyle;
import org.apache.kafka.common.TopicPartition;
import org.immutables.value.Value.Immutable;

import java.util.Set;

@Immutable
@BuildableStyle
public abstract class SubscribeOptions {

    // todo: subscribe / assign options? more options? manual sync?
    // todo: manual user control?
    public abstract Set<TopicPartition> assignment();
}
