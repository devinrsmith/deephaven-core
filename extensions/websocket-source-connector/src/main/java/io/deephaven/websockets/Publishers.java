/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.websockets;

import io.deephaven.stream.StreamPublisher;

public interface Publishers {

    // low level
    static StreamPublisher of(WebsocketPublisher options) {
        return options.publisher();
    }
}
