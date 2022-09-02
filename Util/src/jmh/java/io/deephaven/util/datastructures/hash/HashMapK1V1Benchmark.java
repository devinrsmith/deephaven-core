package io.deephaven.util.datastructures.hash;

import gnu.trove.iterator.TLongLongIterator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

import static io.deephaven.util.datastructures.hash.HashMapBase.DEFAULT_INITIAL_CAPACITY;
import static io.deephaven.util.datastructures.hash.HashMapBase.DEFAULT_LOAD_FACTOR;
import static io.deephaven.util.datastructures.hash.HashMapBase.DEFAULT_NO_ENTRY_VALUE;

@Fork(value = 2, jvmArgs = {"-Xms4G", "-Xmx4G"})
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class HashMapK1V1Benchmark {

    private static final int OPERATIONS = 10000;


    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public long moremur() {
        final HashMapLockFreeK1V1Base map = new HashMapLockFreeK1V1Base(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, DEFAULT_NO_ENTRY_VALUE) {
            @Override
            int hash32(long key) {
                return HashFunctionsForBenchmarking.moremur(key);
            }
        };
        for (int i = 0; i < OPERATIONS; ++i) {
            map.put(i, i);
        }
        return map.get(5253);
    }
}
