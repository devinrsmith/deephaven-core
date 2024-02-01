/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.channel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Objects;

final class ReadableByteChannelNoClose implements ReadableByteChannel {
    private final ReadableByteChannel ch;

    public ReadableByteChannelNoClose(ReadableByteChannel ch) {
        this.ch = Objects.requireNonNull(ch);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return ch.read(dst);
    }

    @Override
    public boolean isOpen() {
        return ch.isOpen();
    }

    @Override
    public void close() {
        // skip
    }
}
