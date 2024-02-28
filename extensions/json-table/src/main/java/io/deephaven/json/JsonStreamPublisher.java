/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.stream.StreamPublisher;

import java.util.Queue;
import java.util.concurrent.Executor;

public interface JsonStreamPublisher extends StreamPublisher {

    void execute(Executor executor, Queue<Source> sources);
}
