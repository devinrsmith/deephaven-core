package io.deephaven.bench;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Warmup;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

@Fork(value = 2, jvmArgs = {"-Xms512M", "-Xmx512M", "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED"})
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class TimeBenchmarks {

    @Benchmark
    public long systemNanoTime() {
        return System.nanoTime();
    }

    @Benchmark
    public long systemCurrentTimeMillis() {
        return System.currentTimeMillis();
    }

    @Benchmark
    public long VMgetNanoTimeAdjustment() {
        return jdk.internal.misc.VM.getNanoTimeAdjustment(0);
    }

    @Benchmark
    public long instantNowNanos() {
        final Instant now = Instant.now();
        return now.getEpochSecond() * 1_000_000_000 + now.getNano();
    }
}
