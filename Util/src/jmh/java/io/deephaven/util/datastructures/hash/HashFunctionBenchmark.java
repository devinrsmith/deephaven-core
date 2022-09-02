package io.deephaven.util.datastructures.hash;

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

@Fork(value = 2, jvmArgs = {"-Xms32G", "-Xmx32G"})
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class HashFunctionBenchmark {

    private static final int OPERATIONS = 10000;

    private static final int SEED = 712093985;

    private static int murmur32(long x) {
        x ^= x >>> 33;
        x *= 0xff51afd7ed558ccdL;
        x ^= x >>> 33;
        x *= 0xc4ceb9fe1a85ec53L;
        x ^= x >>> 33;
        return (int)x;
    }

    /**
     * Returns the 32 high bits of Stafford variant 4 mix64 function as int.
     * See java.util.SplittableRandom#mix32(long).
     */
    private static int SplittableRandom_mix32(long z) {
        z = (z ^ (z >>> 33)) * 0x62a9d9ed799705f5L;
        return (int)(((z ^ (z >>> 28)) * 0xcb24d0a5c88c35b3L) >>> 32);
    }

    private static int SplittableRandom_mix64(long z) {
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return (int)(z ^ (z >>> 31));
    }

    // http://mostlymangling.blogspot.com/2019/01/better-stronger-mixer-and-test-procedure.html
    private static int rrxmrrxmsx_0(long v) {
        v ^= Long.rotateRight(v, 25) ^ Long.rotateRight(v, 50);
        v *= 0xA24BAED4963EE407L;
        v ^= Long.rotateRight(v, 24) ^ Long.rotateRight(v, 49);
        v *= 0x9FB21C651E98DF25L;
        return (int)(v ^ v >>> 28);
    }

    // http://mostlymangling.blogspot.com/2019/12/stronger-better-morer-moremur-better.html
    private static int moremur(long x) {
        x ^= x >>> 27;
        x *= 0x3C79AC492BA7B653L;
        x ^= x >>> 33;
        x *= 0x1C69B3F74AC4AE35L;
        x ^= x >>> 27;
        return (int)x;
    }


//    @Benchmark
//    @OperationsPerInvocation(OPERATIONS)
//    public long overhead() {
//        long x = SEED;
//        for (int i = 0; i < OPERATIONS; ++i) {
//            x = x ^ (x << 32);
//        }
//        return x;
//    }

//    @Benchmark
//    @OperationsPerInvocation(OPERATIONS)
//    public long trove() {
//        long x = SEED;
//        for (int i = 0; i < OPERATIONS; ++i) {
//            x = gnu.trove.impl.HashFunctions.hash(x ^ (x << 32));
//        }
//        return x;
//    }

//    @Benchmark
//    @OperationsPerInvocation(OPERATIONS)
//    public long murmur32() {
//        long x = SEED;
//        for (int i = 0; i < OPERATIONS; ++i) {
//            x = murmur32(x ^ (x << 32));
//        }
//        return x;
//    }

//    @Benchmark
//    @OperationsPerInvocation(OPERATIONS)
//    public long murmur32() {
//        int x = SEED;
//        for (int i = 0; i < OPERATIONS; ++i) {
//            x = murmur32((((long)x) << 32) | x);
//        }
//        return x;
//    }

//    @Benchmark
//    @OperationsPerInvocation(OPERATIONS)
//    public long SplittableRandom_mix32() {
//        int x = SEED;
//        for (int i = 0; i < OPERATIONS; ++i) {
//            x = SplittableRandom_mix32((((long)x) << 32) | x);
//        }
//        return x;
//    }

//    @Benchmark
//    @OperationsPerInvocation(OPERATIONS)
//    public long rrxmrrxmsx_0() {
//        int x = SEED;
//        for (int i = 0; i < OPERATIONS; ++i) {
//            x = rrxmrrxmsx_0((((long)x) << 32) | x);
//        }
//        return x;
//    }

//    @Benchmark
//    @OperationsPerInvocation(OPERATIONS)
//    public long moremur() {
//        int x = SEED;
//        for (int i = 0; i < OPERATIONS; ++i) {
//            x = moremur((((long)x) << 32) | x);
//        }
//        return x;
//    }

    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public long SplittableRandom_mix64() {
        int x = SEED;
        for (int i = 0; i < OPERATIONS; ++i) {
            x = SplittableRandom_mix64((((long)x) << 32) | x);
        }
        return x;
    }
}
