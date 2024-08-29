//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor;

import java.util.List;

public interface EventProcessor<T> {

    EventSpec spec();

    void process(T event, List<Stream> streams, Coordinator coordinator);
}
