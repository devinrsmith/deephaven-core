/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface ObjectService {
    /**
     * Fetch the object.
     *
     * @param ticket the ticket
     * @return the future
     */
    CompletableFuture<FetchedObject> fetchObject(String type, HasTicketId ticket);

    interface MessageStream {
        void onData(ByteBuffer payload, List<? extends HasTicketId> references);

        void onClose();
    }

    MessageStream messageStream(String type, HasTicketId ticket, MessageStream stream);
}
