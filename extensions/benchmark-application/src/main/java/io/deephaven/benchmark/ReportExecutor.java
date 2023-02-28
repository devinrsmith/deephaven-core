package io.deephaven.benchmark;

import io.deephaven.base.clock.Clock;

import java.io.Closeable;
import java.util.List;
import java.util.function.Supplier;

public interface ReportExecutor extends Closeable {

    static ReportExecutor of(String name, Clock clock) {
        return new ReportImpl(name, clock);
    }

    void timestamp(String name);

    Timer timer(String name);

    MultistageTimer multistageTimer(String name);

    /**
     * Execute {@code runnable} against a timer. Equivalent to
     * 
     * <pre>
     * try (final Timer timer = timer(name)) {
     *     runnable.run();
     * }
     * </pre>
     *
     * @param name the name
     * @param runnable the runnable
     */
    void time(String name, Runnable runnable);

    /**
     * Execute {@code supplier} against a timer. Equivalent to
     * 
     * <pre>
     * try (final Timer timer = timer(name)) {
     *     return supplier.get();
     * }
     * </pre>
     *
     * @param name the name
     * @param supplier the supplier
     */
    <T> T time(String name, Supplier<T> supplier);

    @Override
    void close();

    List<Metric> metrics();

    interface Timer extends Closeable {

        @Override
        void close();
    }

    interface MultistageTimer extends Closeable {

        void mark(String name);

        void mark(String name, Runnable runnable);

        <T> T mark(String name, Supplier<T> supplier);

        @Override
        void close();
    }
}
