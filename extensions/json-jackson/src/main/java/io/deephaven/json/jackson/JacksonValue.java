//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value;

@Value.Immutable
@SimpleStyle
public abstract class JacksonValue {

    public static JacksonValue of(final io.deephaven.json.Value options) {
        return null;
    }

    @Value.Parameter
    public abstract io.deephaven.json.Value options();

    public final JacksonProvider provider() {
        return JacksonProvider.of(options());
    }

    public final JacksonProvider provider(final JsonFactory factory) {
        return JacksonProvider.of(options(), factory);
    }

    public final JacksonValue2 array() {
        return JacksonValue2.array(options());
    }

    public final JacksonValue2 stream() {
        return JacksonValue2.stream(options());
    }

    public final JacksonValue2 entries() {
        throw new RuntimeException("todo"); // only valid for object entries type
    }
}
