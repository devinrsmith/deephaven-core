package io.deephaven.chunk;


import io.deephaven.chunk.ChunksProvider.Transaction;
import io.deephaven.time.DateTimeUtils;

import java.time.Instant;
import java.util.Collection;

public class ComplexStuff {

    // {
    //  "timestamp": "...",
    //  "sym": "sym1",
    //  "bids": [[13.98, 100], [13.99, 25]],
    //  "asks": [[14.01, 33], [14.02, 200], [14.03, 500]]
    //}
    //{
    //  "timestamp": "...",
    //  "sym": "sym2",
    //  "bids": [[13.98, 100], [13.99, 25]],
    //  "asks": [[14.01, 33], [14.02, 200], [14.03, 500]]
    //}
    //{
    //  "timestamp": "...",
    //  "sym": "sym3",
    //  "bids": [[13.98, 100], [13.99, 25]],
    //  "asks": [[14.01, 33], [14.02, 200], [14.03, 500]]
    //}
    //{
    //  "timestamp": "...",
    //  "sym": "sym4",
    //  "bids": [[13.98, 100], [13.99, 25]],
    //  "asks": [[14.01, 33], [14.02, 200], [14.03, 500]]
    //}

    public void handle(TopLevelMessage message, ChunksProvider base, ChunksProvider bids, ChunksProvider asks) {
        try (final Transaction tx = base.tx()) {
            for (SymEntry entry : message.entries()) {
                handleSymEntryImpl(tx, entry, bids, asks);
            }
            tx.commit();
        }
    }

    public void handleSymEntry(SymEntry symEntry, ChunksProvider base, ChunksProvider bids, ChunksProvider asks) {
        try (final Transaction tx = base.tx()) {
            handleSymEntryImpl(tx, symEntry, bids, asks);
            tx.commit();
        }
    }

    private void handleSymEntryImpl(Transaction tx, SymEntry symEntry, ChunksProvider bids, ChunksProvider asks) {
        handlePriceSizes(symEntry.bids(), bids);
        handlePriceSizes(symEntry.asks(), asks);

        final WritableChunks chunks = tx.take(1);
        chunks.out().get(0).asWritableLongChunk().add(DateTimeUtils.epochNanos(symEntry.timestamp()));
        chunks.out().get(1).<String>asWritableObjectChunk().add(symEntry.sym());
        tx.complete(chunks, 1);
    }

    public void handlePriceSizes(Collection<PriceSize> priceSizes, ChunksProvider provider) {
        // todo: parent tx
        try (final Transaction tx = provider.tx()) {
            for (PriceSize priceSize : priceSizes) {
                final WritableChunks chunks = tx.take(1);
                chunks.out().get(0).asWritableDoubleChunk().add(priceSize.price());
                chunks.out().get(1).asWritableIntChunk().add(priceSize.size());
                tx.complete(chunks, 1);
            }
            tx.commit();
        }
    }

    public interface TopLevelMessage {
        Collection<SymEntry> entries();
    }

    public interface SymEntry {
        Instant timestamp();
        String sym();
        Collection<PriceSize> bids();

        Collection<PriceSize> asks();
    }

    public interface PriceSize {
        double price();
        int size();
    }
}
