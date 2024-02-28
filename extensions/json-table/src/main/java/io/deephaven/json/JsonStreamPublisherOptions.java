/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

@Immutable
@BuildableStyle
public abstract class JsonStreamPublisherOptions {

    public static Builder builder() {
        return ImmutableJsonStreamPublisherOptions.builder();
    }

    public abstract ValueOptions options();

    public abstract boolean multiValueSupport();

    public abstract int chunkSize();

    public final JsonStreamPublisher execute() {
        return JsonStreamPublisherProvider.serviceLoader().of(this);
    }

    public interface Builder {
        Builder options(ValueOptions options);

        Builder multiValueSupport(boolean multiValueSupport);

        Builder chunkSize(int chunkSize);

        JsonStreamPublisherOptions build();
    }
}
