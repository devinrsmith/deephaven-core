package io.deephaven.parquet.table.util;

import io.deephaven.base.verify.Assert;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public final class ByteBufferAsyncResponseTransformer<ResponseT> implements AsyncResponseTransformer<ResponseT, ByteBuffer> {

    private final int bufferSize;
    private volatile CompletableFuture<ByteBuffer> cf;

    ByteBufferAsyncResponseTransformer(final int bufferSize) {
        this.bufferSize = bufferSize;
    }

    @Override
    public CompletableFuture<ByteBuffer> prepare() {
        cf = new CompletableFuture<>();
        return cf;
    }

    @Override
    public void onResponse(final ResponseT response) {
        // No need to store the response object as we are only interested in the byte buffer
    }

    @Override
    public void onStream(final SdkPublisher<ByteBuffer> publisher) {
        // TODO Can be improved with a buffer pool
        publisher.subscribe(new ByteBufferSubscriber(cf, ByteBuffer.allocate(bufferSize)));
    }

    @Override
    public void exceptionOccurred(final Throwable throwable) {
        cf.completeExceptionally(throwable);
    }

    final static class ByteBufferSubscriber implements Subscriber<ByteBuffer> {
        private final CompletableFuture<ByteBuffer> resultFuture;
        private Subscription subscription;
        private final ByteBuffer byteBuffer;

        ByteBufferSubscriber(CompletableFuture<ByteBuffer> resultFuture, ByteBuffer byteBuffer) {
            this.resultFuture = resultFuture;
            this.byteBuffer = byteBuffer;
        }

        @Override
        public void onSubscribe(final Subscription s) {
            if (subscription != null) {
                s.cancel();
                return;
            }
            subscription = s;
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(final ByteBuffer responseBytes) {
            // Assuming responseBytes will fit in the buffer
            Assert.assertion(responseBytes.remaining() <= byteBuffer.remaining(),
                    "responseBytes.remaining() <= byteBuffer.remaining()");
            byteBuffer.put(responseBytes);
            subscription.request(1);
        }

        @Override
        public void onError(final Throwable throwable) {
            resultFuture.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            resultFuture.complete(byteBuffer.asReadOnlyBuffer());
        }
    }
}
