/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

import java.util.Set;

@Immutable
@BuildableStyle
public abstract class TopicPartitions {

    public abstract String topic();

    public abstract Set<Integer> partitions();
}
