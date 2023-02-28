package io.deephaven.benchmark;

import io.deephaven.base.clock.Clock;
import io.deephaven.benchmark.MultistageTimer.Builder;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

final class ReportImpl implements ReportExecutor {
    private final String name;
    private final Clock clock;
    private final Map<String, Metricable> metrics;

    public ReportImpl(String name, Clock clock) {
        this.name = Objects.requireNonNull(name);
        this.clock = Objects.requireNonNull(clock);
        this.metrics = new LinkedHashMap<>();
    }

    @Override
    public void timestamp(String name) {
        final TimestampImpl timestamp = new TimestampImpl(name);
        if (metrics.putIfAbsent(name, timestamp) != null) {
            throw new IllegalStateException("todo");
        }
        timestamp.init();
    }

    @Override
    public Timer timer(String name) {
        final TimerImpl timer = new TimerImpl(name);
        if (metrics.putIfAbsent(name, timer) != null) {
            throw new IllegalStateException("todo");
        }
        timer.init();
        return timer;
    }

    @Override
    public MultistageTimer multistageTimer(String name) {
        final MultistageTimerImpl timer = new MultistageTimerImpl(name);
        if (metrics.putIfAbsent(name, timer) != null) {
            throw new IllegalStateException("todo");
        }
        timer.init();
        return timer;
    }

    @Override
    public List<Metric> metrics() {
        return metrics.values().stream().filter(Metricable::isFinished).map(Metricable::metric)
                .collect(Collectors.toList());
    }

    @Override
    public void close() {

    }

    @Override
    public void time(String name, Runnable runnable) {
        try (final Timer _timer = timer(name)) {
            runnable.run();
        }
    }

    @Override
    public <T> T time(String name, Supplier<T> supplier) {
        try (final Timer _timer = timer(name)) {
            return supplier.get();
        }
    }

    private void finished(TimerImpl timer) {
        // todo
    }

    private void finished(MultistageTimerImpl timer) {
        // todo
    }

    private class TimestampImpl implements Metricable {
        private final String name;
        private Instant timestamp;

        public TimestampImpl(String name) {
            this.name = Objects.requireNonNull(name);
        }

        void init() {
            timestamp = clock.instantMillis();
        }

        @Override
        public boolean isFinished() {
            return true;
        }

        @Override
        public Timestamp metric() {
            return Timestamp.of(name, timestamp);
        }
    }

    private class TimerImpl implements Timer, Metricable {
        private final String name;
        private long startNanos;
        private long stopNanos;

        public TimerImpl(String name) {
            this.name = Objects.requireNonNull(name);
        }

        void init() {
            startNanos = System.nanoTime();
        }

        public boolean isFinished() {
            return stopNanos != 0;
        }

        @Override
        public synchronized void close() {
            long nowNanos = System.nanoTime();
            if (isFinished()) {
                return;
            }
            stopNanos = nowNanos;
            finished(this);
        }

        @Override
        public io.deephaven.benchmark.Timer metric() {
            return io.deephaven.benchmark.Timer.of(name, Duration.ofNanos(stopNanos - startNanos));
        }
    }

    private class MultistageTimerImpl implements MultistageTimer, Metricable {
        private final String name;
        private long startNanos;
        private long stopNanos;
        private final Map<String, Long> stages;

        public MultistageTimerImpl(String name) {
            this.name = Objects.requireNonNull(name);
            this.stages = new LinkedHashMap<>();
        }

        void init() {
            startNanos = System.nanoTime();
        }

        public boolean isFinished() {
            return stopNanos != 0;
        }

        @Override
        public synchronized void mark(String name) {
            long nanos = System.nanoTime();
            if (stages.putIfAbsent(name, nanos) != null) {
                throw new IllegalStateException("todo");
            }
        }

        @Override
        public void mark(String name, Runnable runnable) {
            runnable.run();
            mark(name);
        }

        @Override
        public <T> T mark(String name, Supplier<T> supplier) {
            final T t = supplier.get();
            mark(name);
            return t;
        }

        @Override
        public synchronized void close() {
            long nowNanos = System.nanoTime();
            if (isFinished()) {
                return;
            }
            stopNanos = nowNanos;
            finished(this);
        }

        @Override
        public io.deephaven.benchmark.MultistageTimer metric() {
            final Builder builder = io.deephaven.benchmark.MultistageTimer.builder().name(name);
            long prevNanos = startNanos;
            for (Entry<String, Long> e : stages.entrySet()) {
                final long currentNanos = e.getValue();
                builder.putStages(e.getKey(), Duration.ofNanos(currentNanos - prevNanos));
                prevNanos = currentNanos;
            }
            return builder.build();
        }
    }

    private interface Metricable {
        boolean isFinished();

        Metric metric();
    }
}
