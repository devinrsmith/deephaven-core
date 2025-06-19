//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.engine.context.ExecutionContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

public final class JobSchedulerFuture {

    public static <CONTEXT_TYPE extends JobScheduler.JobThreadContext> CompletableFuture<Status> iterateParallel(
            final JobScheduler scheduler,
            @Nullable final ExecutionContext executionContext,
            @Nullable final LogOutputAppendable description,
            @NotNull final Supplier<CONTEXT_TYPE> taskThreadContextFactory,
            final int start,
            final int count,
            @NotNull final JobScheduler.IterateResumeAction<CONTEXT_TYPE> action,
            @NotNull final Runnable onComplete,
            @NotNull final Consumer<Exception> onError) {
        final CallbackWrapper c = new CallbackWrapper(onComplete, onError);
        scheduler.iterateParallel(executionContext, description, taskThreadContextFactory, start, count, action, c, c);
        return c.futureCopy();
    }

    public enum Status {
        ON_COMPLETED_OK, ON_ERROR_OK, ON_ERROR_BAD
    }

    private static final class CallbackWrapper implements Runnable, Consumer<Exception> {

        private final Lock lock = new ReentrantLock();

        private final CompletableFuture<Status> future;
        private final Runnable onComplete;
        private final Consumer<Exception> onError;

        private boolean onCompleteInvoked;
        private boolean onCompleteSuccess;
        private boolean onErrorInvoked;

        private CallbackWrapper(Runnable onComplete, Consumer<Exception> onError) {
            this.onComplete = Objects.requireNonNull(onComplete);
            this.onError = Objects.requireNonNull(onError);
            this.future = new CompletableFuture<>();
        }

        public CompletableFuture<Status> futureCopy() {
            return future.copy();
        }

        /**
         * To only be invoked as the {@code onComplete} part of a {@link JobScheduler} iteration.
         *
         * @see JobScheduler#iterateSerial(ExecutionContext, LogOutputAppendable, Supplier, int, int,
         *      JobScheduler.IterateResumeAction, Runnable, Consumer)
         * @see JobScheduler#iterateParallel(ExecutionContext, LogOutputAppendable, Supplier, int, int,
         *      JobScheduler.IterateAction, Runnable, Consumer)
         * @see JobScheduler#iterateParallel(ExecutionContext, LogOutputAppendable, Supplier, int, int,
         *      JobScheduler.IterateResumeAction, Runnable, Consumer)
         */
        @Override
        public void run() {
            lock.lock();
            try {
                if (onErrorInvoked) {
                    throw new IllegalStateException("Can't invoke onComplete after onError has been invoked");
                }
                if (onCompleteInvoked) {
                    throw new IllegalStateException("Can't invoke onComplete multiple times");
                }
                onCompleteInvoked = true;
                onComplete.run();
                onCompleteSuccess = true;
            } finally {
                lock.unlock();
            }
            // Only invoked when onComplete.run() completes successfully, and the _last_ thing in this method
            future.complete(Status.ON_COMPLETED_OK);
        }

        /**
         * To only be invoked as the {@code onError} part of a {@link JobScheduler} iteration.
         *
         * @see JobScheduler#iterateSerial(ExecutionContext, LogOutputAppendable, Supplier, int, int,
         *      JobScheduler.IterateResumeAction, Runnable, Consumer)
         * @see JobScheduler#iterateParallel(ExecutionContext, LogOutputAppendable, Supplier, int, int,
         *      JobScheduler.IterateAction, Runnable, Consumer)
         * @see JobScheduler#iterateParallel(ExecutionContext, LogOutputAppendable, Supplier, int, int,
         *      JobScheduler.IterateResumeAction, Runnable, Consumer)
         */
        @Override
        public void accept(final Exception e) {
            boolean invoked = false;
            boolean success = false;
            lock.lock();
            try {
                if (onCompleteSuccess) {
                    throw new IllegalStateException("Can't invoke onError after onComplete success");
                }
                if (onErrorInvoked) {
                    throw new IllegalStateException("Can't invoke onError multiple times");
                }
                onErrorInvoked = invoked = true;
                onError.accept(e);
                success = true;
            } finally {
                lock.unlock();
                if (invoked) {
                    future.complete(success ? Status.ON_ERROR_OK : Status.ON_ERROR_BAD);
                }
            }
        }
    }
}
