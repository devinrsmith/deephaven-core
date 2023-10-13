package io.deephaven.chunk;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public abstract class ChunksProviderBase implements ChunksProvider, Closeable {

    private final Set<Transaction> outstanding = new HashSet<>();
    private boolean closed;

    @Override
    public synchronized Transaction tx() {
        if (closed) {
            throw new IllegalStateException("closed");
        }
        final Transaction txn = txImpl();
        outstanding.add(txn);
        return txn;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        final Iterator<Transaction> it = outstanding.iterator();
        while (it.hasNext()) {
            final Transaction txn = it.next();
        }
    }

    protected abstract Transaction txImpl();

    protected void onClosed(Transaction txn) {
        synchronized (outstanding) {
            outstanding.remove(txn);
        }
    }
}
