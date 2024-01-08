/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import javax.annotation.Nullable;

@Immutable
@SimpleStyle
abstract class OffsetCommitted implements Offset {

    @Parameter
    @Nullable
    public abstract Offset fallback();
}
