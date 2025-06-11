//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.json.Value;

final class JacksonStreamProvider extends JacksonIteratorProviderBase {

    public static JacksonStreamProvider of(final Value options) {
        return new JacksonStreamProvider(Mixin.of(options));
    }

    JacksonStreamProvider(Mixin<?> mixin) {
        super(mixin);
    }

    @Override
    public JacksonIterator iterator(final JsonParser parser, final int bufferSize) {
        return new JacksonStreamIterator(processor, parser, bufferSize);
    }
}
