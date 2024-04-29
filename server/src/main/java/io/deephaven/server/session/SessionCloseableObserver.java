//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.io.Closeable;

import static io.deephaven.extensions.barrage.util.GrpcUtil.safelyComplete;

public abstract class SessionCloseableObserver<T> {
    protected final SessionState session;
    protected final StreamObserver<T> responseObserver;
    private boolean isClosed = false;

    public SessionCloseableObserver(
            final SessionState session,
            final StreamObserver<T> responseObserver) {
        this.session = session;
        this.responseObserver = responseObserver;
        ((ServerCallStreamObserver<T>) responseObserver).setOnCancelHandler(this::close);
    }

    public final void close() {
        synchronized (this) {
            if (isClosed) {
                return;
            }
            isClosed = true;
        }

        onClose();
        safelyComplete(responseObserver);
    }

    /**
     * Override this to perform any additional specific clean up that must be performed.
     */
    protected void onClose() {

    }
}
