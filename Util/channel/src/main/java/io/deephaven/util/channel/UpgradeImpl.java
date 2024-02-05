/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.channel;

import io.deephaven.util.channel.SeekableChannelsProvider.Upgrade;

import java.util.Objects;

final class UpgradeImpl implements Upgrade {
    private final SeekableChannelContext context;

    public UpgradeImpl(SeekableChannelContext context) {
        this.context = Objects.requireNonNull(context);
    }

    @Override
    public SeekableChannelContext context() {
        return context;
    }

    @Override
    public void close() {
        context.close();
    }
}
