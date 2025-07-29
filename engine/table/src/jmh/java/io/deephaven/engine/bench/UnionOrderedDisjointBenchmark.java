//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.bench;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import org.apache.commons.math3.distribution.GeometricDistribution;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(3)
public class UnionOrderedDisjointBenchmark {
    @Param
    RowSetParams rowSetParams;

    @Param({"10", "100", "1000"})
    int nToUnion;

    @Param({"1000000000"})
    int totalRows;

    private RowSet[] toUnion;

    public enum RowSetParams {
        // @formatter:off
//        RANDOM(1, 1),

        MIXED_4(4, 4),
        MIXED_7(7, 7),
        MIXED_10(10, 10),
        MIXED_13(13, 13),
        MIXED_16(16, 16),

        DENSE4_1(1, 5),
        DENSE4_4(4, 8),
        DENSE4_7(7, 11),
        DENSE4_10(10, 14),
        DENSE4_13(13, 17),

        SPARSE4_1(5, 1),
        SPARSE4_4(8, 4),
        SPARSE4_7(11, 7),
        SPARSE4_10(14, 10),
        SPARSE4_13(17, 13),

        DENSE7_1(1, 8),
        DENSE7_4(4, 11),
        DENSE7_7(7, 14),
        DENSE7_10(10, 17),
        DENSE7_13(13, 20),

        SPARSE7_1(8, 1),
        SPARSE7_4(11, 4),
        SPARSE7_7(14, 7),
        SPARSE7_10(17, 10),
        SPARSE7_13(20, 13),

        DENSE10_1(1, 11),
        DENSE10_4(4, 14),
        DENSE10_7(7, 17),
        DENSE10_10(10, 20),
        DENSE10_13(13, 23),

        SPARSE10_1(11, 1),
        SPARSE10_4(14, 4),
        SPARSE10_7(17, 7),
        SPARSE10_10(20, 10),
        SPARSE10_13(23, 13),
        ;
        // @formatter:on

        private final int zeroMeanRunLength;
        private final int oneMeanRunLength;

        RowSetParams(int zeroMeanRunLengthPow, int oneMeanRunLengthPow) {
            this.zeroMeanRunLength = 1 << zeroMeanRunLengthPow;
            this.oneMeanRunLength = 1 << oneMeanRunLengthPow;
        }
    }


    @Setup(Level.Trial)
    public void setupTrial() {
        toUnion = new RowSet[nToUnion];
        final RandomGenerator rng = new Well19937c(42);
        final GeometricDistribution zeroRunSampler =
                new GeometricDistribution(rng, 1.0 / rowSetParams.zeroMeanRunLength);
        final GeometricDistribution oneRunSampler = new GeometricDistribution(rng, 1.0 / rowSetParams.oneMeanRunLength);
        final double pFirstKey = (double) (rowSetParams.oneMeanRunLength)
                / (rowSetParams.zeroMeanRunLength + rowSetParams.oneMeanRunLength);
        boolean firstKeySet = rng.nextDouble() <= pFirstKey;
        try (final WritableRowSet rowSet = RowSetHelper.createRowSet(
                0,
                totalRows - 1,
                firstKeySet,
                () -> sample(zeroRunSampler),
                () -> sample(oneRunSampler))) {
            toUnion = RowSetHelper.equalKeySplit(rowSet, nToUnion).toArray(WritableRowSet[]::new);
        }
        // does not seem to make a difference
        // if (compactInput) {
        // for (RowSet set : toUnion) {
        // set.writableCast().compact();
        // }
        // }
    }

    @Benchmark
    public WritableRowSet unionSequentialBuilder() {
        return unionSequentialBuilder(Arrays.asList(toUnion));
    }

    @Benchmark
    public WritableRowSet unionInsert() {
        return unionInsert(Arrays.asList(toUnion));
    }

    public static WritableRowSet unionSequentialBuilder(final Collection<RowSet> rowSets) {
        try (final Stream<RowSet> stream = rowSets.stream().filter(RowSet::isNonempty)) {
            final Iterator<RowSet> it = stream.iterator();
            if (!it.hasNext()) {
                return RowSetFactory.empty();
            }
            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
            do {
                builder.appendRowSequence(it.next());
            } while (it.hasNext());
            return builder.build();
        }
    }

    public static WritableRowSet unionInsert(final Collection<RowSet> rowSets) {
        try (final Stream<RowSet> stream = rowSets.stream().filter(RowSet::isNonempty)) {
            final Iterator<RowSet> it = stream.iterator();
            if (!it.hasNext()) {
                return RowSetFactory.empty();
            }
            final WritableRowSet union = it.next().copy();
            try {
                while (it.hasNext()) {
                    union.insert(it.next());
                }
            } catch (final RuntimeException e) {
                try (union) {
                    throw e;
                }
            }
            return union;
        }
    }

    private static int sample(final GeometricDistribution geom) {
        int val;
        do {
            val = geom.sample();
            // TODO: bug that this can ever return 0? also, this is a 9 year old library, there is a newer, commons
            // statistics that may be beetter.
        } while (val <= 0);
        return val;
    }
}
