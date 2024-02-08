package io.deephaven.extensions.s3;

import io.deephaven.util.channel.SeekableChannelContext;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Context object used to store read-ahead buffers for efficiently reading from S3.
 */
final class S3ChannelContext implements SeekableChannelContext {
    private static final Logger log = LoggerFactory.getLogger(S3ChannelContext.class);
    private static final long UNINITIALIZED_SIZE = -1;


    private final S3AsyncClient client;
    final S3Instructions instructions;

    private S3Uri uri;

    /**
     * Used to cache recently fetched fragments for faster lookup
     */
    private final Request[] requests;

    /**
     * The size of the object in bytes, stored in context to avoid fetching multiple times
     */
    private long size;

    private long numFragmentsInObject;

    private boolean closed;

    S3ChannelContext(S3AsyncClient client, S3Instructions instructions) {
        this.client = Objects.requireNonNull(client);
        this.instructions = Objects.requireNonNull(instructions);
        requests = new Request[instructions.maxCacheSize()];
        size = UNINITIALIZED_SIZE;
    }

    private int getIndex(final long fragmentIndex) {
        // TODO(deephaven-core#5061): Experiment with LRU caching
        return (int) (fragmentIndex % requests.length);
    }

    private void assume(S3Uri uri) {
        if (this.uri == null) {
            this.uri = Objects.requireNonNull(uri);
        } else {
            if (!this.uri.equals(uri)) {
                throw new IllegalStateException("inconsistent URI");
            }
        }
    }

    public long size(S3Uri uri) throws IOException {
        assume(uri);
        populateSize();
        return size;
    }

    public int fill(S3Uri uri, final long position, ByteBuffer dest) throws IOException {
        assume(uri);
        populateSize();
        // Send async read requests for current fragment as well as read ahead fragments
        final long fragmentIx = fragmentIndex(position);
        final int numReadAheadFragments = (int) Math.min(
                instructions.readAheadCount(),
                numFragmentsInObject - fragmentIx - 1);
        final Request request = getOrCreateRequest(fragmentIx);
        for (int i = -1; i < numReadAheadFragments; ++i) {
            getOrCreateRequest(fragmentIx + i + 1);
        }
        int filled = request.fill(position, dest);
        // if dest has more remaining, and is request is immediately available, we can fill more
        for (int i = 0; i < numReadAheadFragments && dest.hasRemaining(); ++i) {
            final Request ahead = getRequest(fragmentIx + i + 1);
            if (!ahead.isDone()) {
                return filled;
            }
            filled += ahead.fill(position + filled, dest);
        }
        return filled;
    }

    private long fragmentIndex(final long byteNumber) {
        return byteNumber / instructions.fragmentSize();
    }

    public Request getRequest(final long fragmentIndex) {
        final int cacheIdx = getIndex(fragmentIndex);
        Request request = requests[cacheIdx];
        if (request.fragmentIndex != fragmentIndex) {
            throw new IllegalStateException();
        }
        return request;
    }

    public Request getOrCreateRequest(final long fragmentIndex) {
        // todo: error if more than one uri?
        if (closed) {
            throw new IllegalStateException();
        }
        final int cacheIdx = getIndex(fragmentIndex);
        Request request = requests[cacheIdx];
        if (request != null) {
            if (request.fragmentIndex != fragmentIndex) {
                request.cancel();
                requests[cacheIdx] = (request = new Request(fragmentIndex));
            }
        } else {
            requests[cacheIdx] = (request = new Request(fragmentIndex));
        }
        return request;
    }

    long getSize() {
        return size;
    }

    void setSize(final long size) {
        this.size = size;
    }

    @Override
    public void close() {
        log.info("closing context: thread={}, id={}", Thread.currentThread().getId(), System.identityHashCode(this));
        // Cancel all outstanding requests
        for (int i = 0; i < requests.length; i++) {
            if (requests[i] != null) {
                requests[i].cancel();
                requests[i] = null;
            }
        }
        closed = true;
    }

    final class Request implements AsyncResponseTransformer<GetObjectResponse, ByteBuffer> {

        private final long fragmentIndex;
        private final long from;
        private final long to;
        private final CompletableFuture<ByteBuffer> libraryFuture;
        private volatile CompletableFuture<ByteBuffer> rootFuture;
        private GetObjectResponse response;

        private Request(long fragmentIndex) {
            this.fragmentIndex = fragmentIndex;
            from = fragmentIndex * instructions.fragmentSize();
            to = Math.min(from + instructions.fragmentSize(), size) - 1;
            log.info("send: uri={}, ix={}, range={}-{}({})", uri, fragmentIndex, from, to, (to - from + 1));
            libraryFuture = client.getObject(getObjectRequest(), this);
        }

        private int requestLength() {
            return (int) (to - from + 1);
        }

        public GetObjectRequest getObjectRequest() {
            return GetObjectRequest.builder()
                    .bucket(uri.bucket().orElse(null))
                    .key(uri.key().orElse(null))
                    .range("bytes=" + from + "-" + to)
                    .build();
        }

        public void cancel() {
            boolean didCancel = libraryFuture.cancel(true);
            log.info("cancel {}: uri={}, ix={}, range={}-{}({})", didCancel, uri, fragmentIndex, from, to,
                    (to - from + 1));
        }

        public ByteBuffer get() throws IOException {
            try {
                return libraryFuture.get(instructions.readTimeout().toNanos(), TimeUnit.NANOSECONDS).duplicate();
            } catch (final InterruptedException | ExecutionException | TimeoutException | CancellationException e) {
                throw handleS3Exception(e,
                        String.format("fetching fragment %d for %s, context=%d", fragmentIndex, uri,
                                System.identityHashCode(this)));
            }
        }

        public boolean isDone() {
            return libraryFuture.isDone();
        }

        public int fill(long localPosition, ByteBuffer dest) throws IOException {
            final int outOffset = (int) (localPosition - from);
            final int outLength = Math.min((int) (to - localPosition + 1), dest.remaining());
            dest.put(get().position(outOffset).limit(outOffset + outLength));
            return outLength;
        }

        @Override
        public CompletableFuture<ByteBuffer> prepare() {
            final CompletableFuture<ByteBuffer> localFuture = new CompletableFuture<>();
            rootFuture = localFuture;
            return localFuture;
        }

        @Override
        public void onResponse(GetObjectResponse response) {
            this.response = response;
        }

        @Override
        public void onStream(SdkPublisher<ByteBuffer> publisher) {
            publisher.subscribe(new Request.Sub());
        }

        @Override
        public void exceptionOccurred(Throwable error) {
            this.rootFuture.completeExceptionally(error);
        }

        final class Sub implements Subscriber<ByteBuffer> {

            private final CompletableFuture<ByteBuffer> localRoot;
            private final ByteBuffer dest;
            private Subscription subscription;

            Sub() {
                this.localRoot = rootFuture;
                this.dest = ByteBuffer.allocate(requestLength());
            }

            @Override
            public void onSubscribe(Subscription s) {
                if (subscription != null) {
                    s.cancel();
                    return;
                }
                subscription = s;
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ByteBuffer byteBuffer) {
                dest.put(byteBuffer);
                subscription.request(1);
            }

            @Override
            public void onError(Throwable t) {
                localRoot.completeExceptionally(t);
            }

            @Override
            public void onComplete() {
                dest.flip();
                if (dest.remaining() != requestLength()) {
                    throw new IllegalStateException(); // todo
                }
                localRoot.complete(dest.asReadOnlyBuffer());
            }
        }
    }

    private void populateSize() throws IOException {
        if (size != UNINITIALIZED_SIZE) {
            return;
        }
        log.info("head: uri={}, context={}", uri, System.identityHashCode(this));
        // Fetch the size of the file on the first read using a blocking HEAD request, and store it in the context
        // for future use
        final HeadObjectResponse headObjectResponse;
        try {
            headObjectResponse = client
                    .headObject(HeadObjectRequest.builder()
                            .bucket(uri.bucket().orElse(null))
                            .key(uri.key().orElse(null))
                            .build())
                    .get(instructions.readTimeout().toNanos(), TimeUnit.NANOSECONDS);
        } catch (final InterruptedException | ExecutionException | TimeoutException | CancellationException e) {
            throw handleS3Exception(e, String.format("fetching HEAD for file %s", uri));
        }
        this.size = headObjectResponse.contentLength();
        // ceil(size / fragmentSize)
        this.numFragmentsInObject = (size + instructions.fragmentSize() - 1) / instructions.fragmentSize();
    }

    private IOException handleS3Exception(final Exception e, final String operationDescription) {
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            return new IOException(String.format("Thread interrupted while %s", operationDescription), e);
        }
        if (e instanceof ExecutionException) {
            return new IOException(String.format("Execution exception occurred while %s", operationDescription), e);
        }
        if (e instanceof TimeoutException) {
            return new IOException(String.format(
                    "Operation timeout while %s after waiting for duration %s", operationDescription,
                    instructions.readTimeout()), e);
        }
        if (e instanceof CancellationException) {
            return new IOException(String.format("Cancelled an operation while %s", operationDescription), e);
        }
        return new IOException(String.format("Exception caught while %s", operationDescription), e);
    }

}
