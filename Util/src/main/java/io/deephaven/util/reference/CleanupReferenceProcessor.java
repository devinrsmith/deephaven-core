//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.reference;

import io.deephaven.base.reference.CleanupReference;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.Utils;
import io.deephaven.util.annotations.TestUseOnly;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Utility for draining a reference queue of {@link CleanupReference}s and invoking their cleanup methods.
 */
public class CleanupReferenceProcessor {

    @NotNull
    public static CleanupReferenceProcessor getDefault() {
        return Instance.DEFAULT.cleanupReferenceProcessor;
    }

    private enum Instance {
        // @formatter:off
        DEFAULT(new CleanupReferenceProcessor("default", 1000,
                (l, r, e) -> l.warn()
                        .append(Thread.currentThread().getName())
                        .append(": Exception thrown from cleanup of ").append(Utils.REFERENT_FORMATTER, r)
                        .append(": ").append(e)
                        .endl())
        );
        // @formatter:on

        private final CleanupReferenceProcessor cleanupReferenceProcessor;

        Instance(@NotNull final CleanupReferenceProcessor cleanupReferenceProcessor) {
            this.cleanupReferenceProcessor = Require.neqNull(cleanupReferenceProcessor, "cleanupReferenceProcessor");
        }
    }

    private static final Logger log = LoggerFactory.getLogger(CleanupReferenceProcessor.class);

    private static final boolean LOG_CLEANED_REFERENCES =
            Configuration.getInstance().getBooleanWithDefault("CleanupReferenceProcessor.logCleanedReferences", false);

    public interface ExceptionHandler {
        void accept(@NotNull Logger log, @NotNull CleanupReference<?> cleanupReference, @NotNull Exception exception);
    }

    private final String name;
    private final long shutdownCheckDelayMillis;
    private final ExceptionHandler exceptionHandler;

    /**
     * The drain queue from the most recent initialization.
     */
    private volatile DrainQueue drainQueue;

    /**
     * The cleaner thread from the most recent initialization, guarded by the lock on {@code this}.
     */
    private Thread cleanerThread;

    /**
     * Construct a new {@link CleanupReferenceProcessor}.
     *
     * @param name The name of the processor, used for naming threads
     * @param shutdownCheckDelayMillis The frequency with which to check for shutdown
     * @param exceptionHandler Callback for exception handling
     */
    public CleanupReferenceProcessor(
            @NotNull final String name,
            final long shutdownCheckDelayMillis,
            @NotNull final ExceptionHandler exceptionHandler) {
        this.name = Require.neqNull(name, "name");
        this.shutdownCheckDelayMillis = Require.geqZero(shutdownCheckDelayMillis, "shutdownDelayCheckMillis");
        this.exceptionHandler = Require.neqNull(exceptionHandler, "exceptionHandler");
    }

    /**
     * <p>
     * Get the reference queue for this cleaner.
     * <p>
     * On the first call after construction or {@link #resetForUnitTests()}, this method initializes the instance as a
     * side effect. Initialization entails:
     * <ol>
     * <li>Constructing a {@link ReferenceQueue}.</li>
     * <li>Starting a daemon thread that will drain the reference queue and invoke {@link CleanupReference#cleanup()} on
     * any {@link CleanupReference} dequeued.</li>
     * </ol>
     *
     * @return The {@link ReferenceQueue} constructed in the most recent initialization of this
     *         {@link CleanupReferenceProcessor} instance
     */
    public <RT> ReferenceQueue<RT> getReferenceQueue() {
        return getDrainQueue().referenceQueue();
    }

    private DrainQueue getDrainQueue() {
        DrainQueue localQueue;
        if ((localQueue = drainQueue) == null) {
            synchronized (this) {
                if ((localQueue = drainQueue) == null) {
                    drainQueue = localQueue = new DrainQueue(new ReferenceQueue<>());
                    cleanerThread = new Thread(localQueue,
                            "CleanupReferenceProcessor-" + name + "-drainingThread");
                    cleanerThread.setDaemon(true);
                    cleanerThread.start();
                }
            }
        }
        return localQueue;
    }

    /**
     * Registers an object and a cleaning action to run when the object becomes phantom reachable.
     *
     * <p>
     * The most efficient use is to explicitly invoke the cleanup method when the object is closed or no longer needed.
     * The cleaning action is a Runnable to be invoked at most once when the object has become phantom reachable unless
     * it has already been explicitly cleaned. Note that the cleaning action must not refer to the object being
     * registered. If so, the object will not become phantom reachable and the cleaning action will not be invoked
     * automatically.
     *
     * <p>
     * Note: while the caller is encouraged to hold onto the cleanup reference, they are not required to as this cleanup
     * reference processor will hold onto the reference.
     *
     * @param obj the object to monitor
     * @param action a {@code Runnable} to invoke when the object becomes phantom reachable
     * @return a cleanup reference instance
     */
    public <T> CleanupReference<T> register(T obj, Runnable action) {
        return getDrainQueue().register(obj, action);
    }

    /**
     * Registers an object and a cleaning action to run when the object becomes weakly reachable.
     *
     * <p>
     * The most efficient use is to explicitly invoke the cleanup method when the object is closed or no longer needed.
     * The cleaning action is a Runnable to be invoked at most once when the object has become weakly reachable unless
     * it has already been explicitly cleaned. Note that the cleaning action must not refer to the object being
     * registered. If so, the object will not become weakly reachable and the cleaning action will not be invoked
     * automatically.
     *
     * <p>
     * Note: while the caller is encouraged to hold onto the cleanup reference, they are not required to as this cleanup
     * reference processor will hold onto the reference.
     *
     * @param obj the object to monitor
     * @param action a {@code Runnable} to invoke when the object becomes weakly reachable
     * @return a cleanup reference instance
     */
    public <T> CleanupReference<T> registerWeak(T obj, Runnable action) {
        return getDrainQueue().registerWeak(obj, action);
    }

    /**
     * Registers an object and a cleaning action to run when the object becomes softly reachable.
     *
     * <p>
     * The most efficient use is to explicitly invoke the cleanup method when the object is closed or no longer needed.
     * The cleaning action is a Runnable to be invoked at most once when the object has become softly reachable unless
     * it has already been explicitly cleaned. Note that the cleaning action must not refer to the object being
     * registered. If so, the object will not become softly reachable and the cleaning action will not be invoked
     * automatically.
     *
     * <p>
     * Note: while the caller is encouraged to hold onto the cleanup reference, they are not required to as this cleanup
     * reference processor will hold onto the reference.
     *
     * @param obj the object to monitor
     * @param action a {@code Runnable} to invoke when the object becomes softly reachable
     * @return a cleanup reference instance
     */
    public <T> CleanupReference<T> registerSoft(T obj, Runnable action) {
        return getDrainQueue().registerSoft(obj, action);
    }

    /**
     * Reset this instance so that the next call to {@link #getReferenceQueue()} will re-initialize it and provide a new
     * queue. Results in the prompt termination of the daemon thread that may have been draining the existing queue.
     */
    @TestUseOnly
    public final synchronized void resetForUnitTests() {
        drainQueue = null;
        if (cleanerThread != null) {
            cleanerThread.interrupt();
            cleanerThread = null;
        }
    }

    /**
     * Drain the reference queue and call cleanup on any {@link CleanupReference}s dequeued.
     */
    private class DrainQueue implements Runnable {

        private final ReferenceQueue<?> referenceQueue;
        private final Set<RegisteredCleanupReference<?>> registrations;

        private DrainQueue(ReferenceQueue<?> referenceQueue) {
            this.referenceQueue = Objects.requireNonNull(referenceQueue);
            this.registrations = Collections.newSetFromMap(new ConcurrentHashMap<>());
        }

        public <T> ReferenceQueue<T> referenceQueue() {
            // noinspection unchecked
            return (ReferenceQueue<T>) referenceQueue;
        }

        public <T> CleanupReference<T> register(T obj, Runnable action) {
            final PhantomCleanupRef<T> ref = new PhantomCleanupRef<>(obj, referenceQueue(), action);
            registrations.add(ref);
            return ref;
        }

        public <T> CleanupReference<T> registerWeak(T obj, Runnable action) {
            final WeakCleanupRef<T> ref = new WeakCleanupRef<>(obj, referenceQueue(), action);
            registrations.add(ref);
            return ref;
        }

        public <T> CleanupReference<T> registerSoft(T obj, Runnable action) {
            final SoftCleanupRef<T> ref = new SoftCleanupRef<>(obj, referenceQueue(), action);
            registrations.add(ref);
            return ref;
        }

        @Override
        public void run() {
            while (this == drainQueue) {
                final Reference<?> reference;
                try {
                    reference = referenceQueue.remove(shutdownCheckDelayMillis);
                } catch (InterruptedException ignored) {
                    continue;
                }
                if (reference instanceof CleanupReference) {
                    final CleanupReference<?> ref = (CleanupReference<?>) reference;
                    try {
                        if (LOG_CLEANED_REFERENCES) {
                            log.info().append("CleanupReferenceProcessor-").append(name).append(", cleaning ")
                                    .append(Utils.REFERENT_FORMATTER, reference).endl();
                        }
                        ref.cleanup();
                    } catch (Exception e) {
                        exceptionHandler.accept(log, ref, e);
                    }
                    if (ref instanceof RegisteredCleanupReference) {
                        registrations.remove(ref);
                    }
                }
            }
        }
    }

    interface RegisteredCleanupReference<T> extends CleanupReference<T> {

    }

    private static class PhantomCleanupRef<T> extends PhantomReference<T> implements RegisteredCleanupReference<T> {
        private Runnable action;

        PhantomCleanupRef(T referent, ReferenceQueue<? super T> q, Runnable action) {
            super(referent, q);
            this.action = Objects.requireNonNull(action);
            Reference.reachabilityFence(referent);
            Reference.reachabilityFence(q);
        }

        @Override
        public void cleanup() {
            final Runnable cleanup;
            synchronized (this) {
                if (action == null) {
                    return;
                }
                cleanup = action;
                action = null;
            }
            cleanup.run();
        }
    }

    private static class WeakCleanupRef<T> extends WeakReference<T> implements RegisteredCleanupReference<T> {
        private Runnable action;

        WeakCleanupRef(T referent, ReferenceQueue<? super T> q, Runnable action) {
            super(referent, q);
            this.action = Objects.requireNonNull(action);
            Reference.reachabilityFence(referent);
            Reference.reachabilityFence(q);
        }

        @Override
        public void cleanup() {
            final Runnable cleanup;
            synchronized (this) {
                if (action == null) {
                    return;
                }
                cleanup = action;
                action = null;
            }
            cleanup.run();
        }
    }

    private static class SoftCleanupRef<T> extends SoftReference<T> implements RegisteredCleanupReference<T> {
        private Runnable action;

        SoftCleanupRef(T referent, ReferenceQueue<? super T> q, Runnable action) {
            super(referent, q);
            this.action = Objects.requireNonNull(action);
            Reference.reachabilityFence(referent);
            Reference.reachabilityFence(q);
        }

        @Override
        public void cleanup() {
            final Runnable cleanup;
            synchronized (this) {
                if (action == null) {
                    return;
                }
                cleanup = action;
                action = null;
            }
            cleanup.run();
        }
    }
}
