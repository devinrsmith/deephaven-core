package io.deephaven.parquet.table.util;

import io.deephaven.base.verify.Assert;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;

public final class S3BackedSeekableByteChannel implements SeekableByteChannel {

    private static final int CLOSED_SENTINEL = -1;

    private final S3Client s3Client;
    private final S3Uri s3URI;

    private long size;
    private long position;

    S3BackedSeekableByteChannel(@NotNull final S3Client s3Client, @NotNull final S3Uri s3URI, final long size) {
        this.s3Client = s3Client;
        this.s3URI = s3URI;
        this.size = size;
        this.position = 0;
    }

    @Override
    public int read(final ByteBuffer dst) throws IOException {
        final int numBytesToRead = dst.remaining();
        final byte[] readBuf = new byte[numBytesToRead];
        long endPosition = position + numBytesToRead - 1;
        if (endPosition >= size) {
            endPosition = size - 1;
        }
        final String byteRange = "bytes=" + position + "-" + endPosition;
        Assert.eqTrue(s3URI.bucket().isPresent(), "s3URI.bucket().isPresent()");
        Assert.eqTrue(s3URI.key().isPresent(), "s3URI.key().isPresent()");
        final GetObjectRequest rangeObjectRequest = GetObjectRequest.builder()
                .bucket(s3URI.bucket().get())
                .key(s3URI.key().get())
                .range(byteRange)
                .build();

        // Following will create and send a blocking GET request over a pool of shared HTTP connections to S3.
        int totalBytesRead = 0;
        int bytesRead;
        try (final ResponseInputStream<GetObjectResponse> s3InputStream = s3Client.getObject(rangeObjectRequest)) {
            do {
                if ((bytesRead = s3InputStream.read(readBuf, totalBytesRead, numBytesToRead - totalBytesRead)) != -1) {
                    position += bytesRead;
                    totalBytesRead += bytesRead;
                }
            } while (totalBytesRead < numBytesToRead && bytesRead != -1);
        }
        if (totalBytesRead > 0) {
            dst.put(readBuf, 0, totalBytesRead); // TODO Think if we can avoid this extra copy
            return totalBytesRead;
        }
        return bytesRead;
    }

    @Override
    public int write(final ByteBuffer src) throws UnsupportedEncodingException {
        throw new UnsupportedEncodingException("Don't support writing to S3 yet");
    }

    @Override
    public long position() throws ClosedChannelException {
        final long localPosition = position;
        checkClosed(localPosition);
        return localPosition;
    }

    @Override
    public SeekableByteChannel position(final long newPosition) throws ClosedChannelException {
        checkClosed(position);
        position = Math.toIntExact(newPosition);
        return this;
    }

    @Override
    public long size() throws ClosedChannelException {
        checkClosed(position);
        return size;
    }

    @Override
    public SeekableByteChannel truncate(final long newSize) throws ClosedChannelException {
        checkClosed(position);
        size = newSize;
        if (position > newSize) {
            position = newSize;
        }
        return this;
    }

    @Override
    public void close() throws IOException {
        position = CLOSED_SENTINEL;
    }

    private static void checkClosed(final long position) throws ClosedChannelException {
        if (position == CLOSED_SENTINEL) {
            throw new ClosedChannelException();
        }
    }

    @Override
    public boolean isOpen() {
        return position != CLOSED_SENTINEL;
    }
}