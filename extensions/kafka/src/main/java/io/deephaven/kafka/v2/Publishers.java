/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import java.io.Closeable;
import java.util.Collection;
import java.util.function.Function;

public interface Publishers extends Closeable {
    static Publishers of(PublishersOptions<?, ?> options) {
        return options.publishers();
    }

    static <T> T applyAndStart(PublishersOptions<?, ?> options, Function<Collection<? extends Publisher>, T> function) {
        try (final Publishers publishers = of(options)) {
            try {
                final T t = function.apply(publishers.publishers());
                publishers.start();
                return t;
            } catch (Throwable throwable) {
                publishers.errorBeforeStart(throwable);
                throw throwable;
            }
        }
    }

    Collection<? extends Publisher> publishers();

    void start();

    void errorBeforeStart(Throwable t);

    @Override
    void close();
}
