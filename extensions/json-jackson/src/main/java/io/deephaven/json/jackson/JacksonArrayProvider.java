//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.json.Value;

import java.io.IOException;

final class JacksonArrayProvider extends JacksonIteratorProviderBase {

    public static JacksonArrayProvider of(final Value elementOptions) {
        return new JacksonArrayProvider(Mixin.of(elementOptions));
    }

    JacksonArrayProvider(Mixin<?> mixin) {
        super(mixin);
    }

    @Override
    public JacksonIterator iterator(final JsonParser parser, final int bufferSize) throws IOException {
        return new JacksonArrayIterator(processor, parser, bufferSize);
    }
}
