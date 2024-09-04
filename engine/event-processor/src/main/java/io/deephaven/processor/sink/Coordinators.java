//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.sink;

public final class Coordinators {

    public static Coordinator noop() {
        return Noop.NOOP;
    }

    enum Noop implements Coordinator {
        NOOP;

        @Override
        public void sync() {

        }
    }
}
