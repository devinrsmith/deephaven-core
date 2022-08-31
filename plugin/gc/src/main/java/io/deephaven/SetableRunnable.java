package io.deephaven;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public final class SetableRunnable implements Runnable {
    private static final AtomicReferenceFieldUpdater<SetableRunnable, Runnable> CAS =
            AtomicReferenceFieldUpdater.newUpdater(SetableRunnable.class, Runnable.class, "delegate");

    private volatile Runnable delegate;

    public void set(Runnable delegate) {
        if (!CAS.compareAndSet(this, null, delegate)) {
            throw new IllegalStateException("Can only set runnable once");
        }
    }

    @Override
    public void run() {
        Runnable localDelegate = delegate;
        if (localDelegate != null) {
            localDelegate.run();
        }
    }
}
