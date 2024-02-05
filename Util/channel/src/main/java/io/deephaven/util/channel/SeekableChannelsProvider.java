/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.channel;

import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;

public interface SeekableChannelsProvider extends SafeCloseable {

    /**
     * Take the file source path or URI and convert it to a URI object.
     *
     * @param source The file source path or URI
     * @return The URI object
     */
    static URI convertToURI(final String source) {
        final URI uri;
        try {
            uri = new URI(source);
        } catch (final URISyntaxException e) {
            // If the URI is invalid, assume it's a file path
            return new File(source).toURI();
        }
        if (uri.getScheme() == null) {
            // Need to convert to a "file" URI
            return new File(source).toURI();
        }
        return uri;
    }

    static Upgrade upgrade(SeekableChannelsProvider provider, SeekableChannelContext context) {
        if (context != SeekableChannelContext.NULL) {
            return () -> context;
        }
        return new UpgradeImpl(provider.makeSingleUseContext());
    }

    /**
     * Wraps {@link SeekableChannelsProvider#getInputStream(SeekableByteChannel)} in a position-safe manner. To remain
     * valid, the caller must ensure that the resulting input stream isn't re-wrapped by any downstream code in a way
     * that would adversely effect the position (such as wrapping the resulting input stream with buffering).
     *
     * <p>
     * Equivalent to {@code PositionInputStream.of(ch, provider.getInputStream(ch))}.
     *
     * @param provider the provider
     * @param ch the seekable channel
     * @return the position-safe input stream
     * @throws IOException if an IO exception occurs
     * @see PositionInputStream#of(SeekableByteChannel, InputStream)
     */
    static InputStream positionInputStream(SeekableChannelsProvider provider, SeekableByteChannel ch)
            throws IOException {
        return PositionInputStream.of(ch, provider.getInputStream(ch));
    }

    /**
     * Create a new {@link SeekableChannelContext} object for creating read channels via this provider.
     */
    SeekableChannelContext makeContext();

    default SeekableChannelContext makeSingleUseContext() {
        return makeContext();
    }

    /**
     * Check if the given context is compatible with this provider. Useful to test if we can use provided
     * {@code context} object for creating channels with this provider.
     */
    boolean isCompatibleWith(@NotNull SeekableChannelContext channelContext);

    default SeekableByteChannel getReadChannel(@NotNull SeekableChannelContext channelContext, @NotNull String uriStr)
            throws IOException {
        return getReadChannel(channelContext, convertToURI(uriStr));
    }

    SeekableByteChannel getReadChannel(@NotNull SeekableChannelContext channelContext, @NotNull URI uri)
            throws IOException;

    // callers must close this; but it does *not* close channel. guarantees position of channel on close?


    /**
     * Creates an {@link InputStream} from the current position of {@code channel}; closing the resulting input stream
     * does _not_ close the {@code channel}. {@code channel} must have been created by {@code this} channels provider.
     * The caller can't assume the position of {@code channel} after consuming the {@link InputStream}. For use-cases
     * that require the channel's position to be incremented the exact amount the {@link InputStream} has been consumed,
     * use {@link #positionInputStream(SeekableChannelsProvider, SeekableByteChannel)}.
     *
     * Callers assume buffered, either the channel itself, or a buffer around the input.
     *
     * @param channel the channel
     * @return the input stream
     * @throws IOException if an IO exception occurs
     */
    default InputStream getInputStream(SeekableByteChannel channel) throws IOException {
        return Channels.newInputStream(ReadableByteChannelNoClose.of(channel));
    }

    default SeekableByteChannel getWriteChannel(@NotNull final String path, final boolean append) throws IOException {
        return getWriteChannel(Paths.get(path), append);
    }

    SeekableByteChannel getWriteChannel(@NotNull Path path, boolean append) throws IOException;

    interface Upgrade extends Closeable {

        SeekableChannelContext context();

        @Override
        default void close() {}
    }
}
