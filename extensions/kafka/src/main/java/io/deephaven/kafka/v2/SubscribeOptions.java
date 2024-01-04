/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.List;

@Immutable
@BuildableStyle
public abstract class SubscribeOptions {

    // todo "Subscribe" may not be best name here

    public static Builder builder() {
        return ImmutableSubscribeOptions.builder();
    }

    public static SubscribeOptions of(Offsets... elements) {
        return builder().addOffsets(elements).build();
    }

    public static SubscribeOptions beginning(String topic) {
        return of(Offsets.beginning(topic));
    }

    public static SubscribeOptions end(String topic) {
        return of(Offsets.end(topic));
    }

    public abstract List<Offsets> offsets();

    public interface Builder {

        Builder addOffsets(Offsets element);

        Builder addOffsets(Offsets... elements);

        Builder addAllOffsets(Iterable<? extends Offsets> elements);

        SubscribeOptions build();
    }

    @Check
    final void checkNotEmpty() {
        if (offsets().isEmpty()) {
            throw new IllegalArgumentException();
        }
    }
}
