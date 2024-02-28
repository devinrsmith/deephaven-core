/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonFactory;
import io.deephaven.stream.StreamPublisher;

import java.util.List;
import java.util.concurrent.Executor;

public interface JsonStreamPublisher extends StreamPublisher {

    Runnable runnable(List<Source> sources);

    default void execute(Executor executor, List<Source> sources) {
        executor.execute(runnable(sources));
    }
}
