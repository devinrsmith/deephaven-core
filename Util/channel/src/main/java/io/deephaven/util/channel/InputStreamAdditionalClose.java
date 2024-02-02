/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.channel;

import io.deephaven.util.SafeCloseable;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

final class InputStreamAdditionalClose extends FilterInputStream {
    private final SafeCloseable onClose;

    InputStreamAdditionalClose(InputStream in, SafeCloseable onClose) {
        super(in);
        this.onClose = onClose;
    }

    @Override
    public void close() throws IOException {
        try (final SafeCloseable ignored = onClose) {
            super.close();
        }
    }
}
