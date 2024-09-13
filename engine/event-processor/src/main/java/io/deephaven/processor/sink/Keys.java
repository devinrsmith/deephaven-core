//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.sink;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

import java.util.List;

@Immutable
@BuildableStyle
public abstract class Keys {

    // todo: just have simple constructor?
    public static Builder builder() {
        return ImmutableKeys.builder();
    }

    public abstract List<Key<?>> keys();

    public final Keys append(Keys others) {
        return builder()
                .addAllKeys(keys())
                .addAllKeys(others.keys())
                .build();
    }

    public interface Builder {

        Builder addKeys(Key<?> element);

        Builder addKeys(Key<?>... elements);

        Builder addAllKeys(Iterable<? extends Key<?>> elements);

        Keys build();
    }
}
