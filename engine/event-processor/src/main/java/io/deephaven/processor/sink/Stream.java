//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.sink;

import io.deephaven.processor.sink.appender.Appender;

import java.util.List;

public interface Stream {

    // todo: should StreamSpec be on here? probably not

    void ensureRemainingCapacity(long size);

    // todo: add example showing that advance() and advanceAll() can technically be mixed, with motivating (albeit "odd"
    // structure: { "a": 1, "b": 2, "a_rest": [...], "b_rest": [...] }

    // todo: rollback is not a thing, right?

    void advanceAll(); // can only use if row-oriented?


    List<? extends Appender> appenders();
}
