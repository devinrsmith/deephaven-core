/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

@Immutable
@BuildableStyle
public abstract class Options<K, V> {

    public abstract ClientOptions<K, V> consumer();

    public abstract SubscribeOptions subscribe();

    public abstract boolean onDemand(); // todo

}
