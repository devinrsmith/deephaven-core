/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.s3;

import io.deephaven.base.verify.Assert;
import io.deephaven.util.channel.CachedChannelProvider;
import io.deephaven.util.channel.SeekableChannelContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Uri;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.Objects;


/**
 * {@link SeekableByteChannel} class used to fetch objects from S3 buckets using an async client with the ability to
 * read ahead and cache fragments of the object.
 */
final class S3SeekableByteChannel implements SeekableByteChannel, CachedChannelProvider.ContextHolder {
    private static final Logger log = LoggerFactory.getLogger(S3SeekableByteChannel.class);

    private static final long CLOSED_SENTINEL = -1;

    private final S3Uri uri;

    /**
     * The {@link SeekableChannelContext} object used to cache read-ahead buffers for efficiently reading from S3. This
     * is set before the read and cleared when closing the channel.
     */
    private S3ChannelContext context;

    private long position;

    S3SeekableByteChannel(S3Uri uri) {
        this.uri = Objects.requireNonNull(uri);
    }

    /**
     * @param channelContext The {@link SeekableChannelContext} object used to cache read-ahead buffers for efficiently
     *        reading from S3. An appropriate channel context should be set before the read and should be cleared after
     *        the read is complete via {@link io.deephaven.util.channel.SeekableChannelsProvider#makeContext()}. A
     *        {@code null} parameter value is equivalent to clearing the context.
     */
    @Override
    public void setContext(@Nullable final SeekableChannelContext channelContext) {
        if (channelContext != null && !(channelContext instanceof S3ChannelContext)) {
            throw new IllegalArgumentException("Unsupported channel context " + channelContext);
        }
        this.context = (S3ChannelContext) channelContext;
    }

    @Override
    public int read(@NotNull final ByteBuffer destination) throws IOException {
        Assert.neqNull(context, "channelContext");
        checkClosed(position);
        if (position >= context.size(uri)) {
            // We are finished reading
            return -1;
        }
        if (!destination.hasRemaining()) {
            return 0;
        }
        final int filled = context.fill(uri, position, destination);
        position += filled;
        return filled;
    }

    @Override
    public int write(final ByteBuffer src) {
        throw new NonWritableChannelException();
    }

    @Override
    public long position() throws ClosedChannelException {
        final long localPosition = position;
        checkClosed(localPosition);
        return localPosition;
    }

    @Override
    public SeekableByteChannel position(final long newPosition) throws ClosedChannelException {
        if (newPosition < 0) {
            throw new IllegalArgumentException("newPosition cannot be < 0, provided newPosition=" + newPosition);
        }
        checkClosed(position);
        position = newPosition;
        return this;
    }

    @Override
    public long size() throws IOException {
        checkClosed(position);
        Assert.neqNull(context, "channelContext");
        return context.size(uri);
    }

    @Override
    public SeekableByteChannel truncate(final long size) {
        throw new NonWritableChannelException();
    }

    @Override
    public boolean isOpen() {
        return position != CLOSED_SENTINEL;
    }

    @Override
    public void close() {
        position = CLOSED_SENTINEL;
    }

    private static void checkClosed(final long position) throws ClosedChannelException {
        if (position == CLOSED_SENTINEL) {
            throw new ClosedChannelException();
        }
    }
}
