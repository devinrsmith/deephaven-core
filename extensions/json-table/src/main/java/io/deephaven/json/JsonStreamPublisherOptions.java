/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.json.jackson.JacksonStreamPublisher;
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
        // This is the only reference from io.deephaven.json into io.deephaven.json.jackson. If we want to break out
        // io.deephaven.json.jackson into a separate project, we'd probably want a ServiceLoader pattern here to choose
        // a default implementation.
        return JacksonStreamPublisher.of(this);
    }

    public interface Builder {
        Builder options(ValueOptions options);

        Builder multiValueSupport(boolean multiValueSupport);

        Builder chunkSize(int chunkSize);

        JsonStreamPublisherOptions build();
    }
}
