/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import java.io.Closeable;
import java.util.Collection;
import java.util.function.Function;

public interface Publishers extends Closeable {
    /**
     * Creates a publishers.
     *
     * @param options
     * @return
     */
    static Publishers of(PublishersOptions<?, ?> options) {
        return options.publishers();
    }

    /**
     * Provides a safe usage pattern for transforming {@link Publishers#publishers()}.
     *
     * @param options the options
     * @param function the function
     * @return the function return
     * @param <T> the function return type
     */
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
