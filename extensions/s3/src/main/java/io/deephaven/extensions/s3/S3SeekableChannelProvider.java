/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.s3;

import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;

import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;

/**
 * {@link SeekableChannelsProvider} implementation that is used to fetch objects from AWS S3 instances.
 */
final class S3SeekableChannelProvider implements SeekableChannelsProvider {

    private final S3AsyncClient s3AsyncClient;
    private final S3Instructions s3Instructions;
    private final BufferPool bufferPool;

    S3SeekableChannelProvider(@NotNull final S3Instructions s3Instructions) {
        final SdkAsyncHttpClient asyncHttpClient = AwsCrtAsyncHttpClient.builder()
                .maxConcurrency(s3Instructions.maxConcurrentRequests())
                .connectionTimeout(s3Instructions.connectionTimeout())
                .build();
        // TODO(deephaven-core#5062): Add support for async client recovery and auto-close
        // TODO(deephaven-core#5063): Add support for caching clients for re-use
        final S3AsyncClientBuilder builder = S3AsyncClient.builder()
                .region(Region.of(s3Instructions.awsRegionName()))
                .httpClient(asyncHttpClient);
        s3Instructions.credentialsProvider().ifPresent(builder::credentialsProvider);
        s3Instructions.endpoint().ifPresent(builder::endpointOverride);
        this.s3AsyncClient = builder.build();
        this.s3Instructions = s3Instructions;
        this.bufferPool = new SegmentedBufferPool(s3Instructions.fragmentSize());
    }

    @Override
    public SeekableByteChannel getReadChannel(@NotNull final SeekableChannelContext channelContext,
            @NotNull final URI uri) {
        // context is unused here, will be set before reading from the channel
        return new S3SeekableByteChannel(uri, s3AsyncClient, s3Instructions, bufferPool);
    }

    @Override
    public SeekableChannelContext makeContext() {
        return new S3SeekableByteChannel.S3ChannelContext(s3Instructions.maxCacheSize());
    }

    @Override
    public boolean isCompatibleWith(@NotNull final SeekableChannelContext channelContext) {
        // A null context implies no caching or read ahead
        return channelContext == SeekableChannelContext.NULL
                || channelContext instanceof S3SeekableByteChannel.S3ChannelContext;
    }

    @Override
    public SeekableByteChannel getWriteChannel(@NotNull final Path path, final boolean append) {
        throw new UnsupportedOperationException("Writing to S3 is currently unsupported");
    }

    public void close() {
        s3AsyncClient.close();
    }
}