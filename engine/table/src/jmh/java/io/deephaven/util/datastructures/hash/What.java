package io.deephaven.util.datastructures.hash;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSet.Iterator;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListener;
import io.deephaven.engine.table.impl.select.IncrementalReleaseFilter;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.PrintListener;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.junit4.EngineCleanup;
import org.jetbrains.annotations.Nullable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Fork(value = 2, jvmArgs = {"-Xms16G", "-Xmx16G"})
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class What {

    static {
        System.setProperty("Configuration.rootFile", "dh-tests.prop");
        System.setProperty("workspace", ".");
        System.setProperty("UnionRedirection.allocationUnit", "2199023255552");
    }

    private EngineCleanup engine;
    private UpdateGraphProcessor ugp;
    private IncrementalReleaseFilter filter;
    private Table ms;
    private int numCycles;
    private Listener listener;

    private class Listener extends InstrumentedTableUpdateListener {

        private long state = 0;

        public Listener(@Nullable String description) {
            super("Listener");
        }

        @Override
        public void onUpdate(TableUpdate upstream) {
            state += upstream.added().size();
        }

        @Override
        protected void onFailureInternal(Throwable originalException, Entry sourceEntry) {

        }
    }

    @Setup(Level.Invocation)
    public void setup() throws Exception {
        engine = new EngineCleanup();
        engine.setUp();
        ugp = UpdateGraphProcessor.DEFAULT;

        final int componentSize = 2000000;
        final int numBuckets = 2000;
        final int numParts = 10;
        final int remainingRows = 1000000;
        final int tableSize = numParts * componentSize;
        final int initialSize = tableSize - remainingRows;
        final int cycleIncrement = 20000;
        numCycles = remainingRows / cycleIncrement;
        if (numCycles != 50) {
            throw new IllegalStateException();
        }

        // create the initial table
        ugp.startCycleForUnitTests();
        ms = create(componentSize, numBuckets, numParts, initialSize, cycleIncrement);
        listener = new Listener("test");
        ms.listenForUpdates(listener);
        ugp.completeCycleForUnitTests();
    }

    private Table create(int componentSize, int numBuckets, int numParts, int initialSize, int cycleIncrement) {
        final Table base = TableTools.emptyTable(componentSize).update("Bucket = ii % " + numBuckets, "Time = ii");
        final List<Table> parts = new ArrayList<>();
        for (int i = 0; i < numParts; ++i) {
            parts.add(base.update("Tab=" + i));
        }
        final Table m = TableTools.merge(parts);
        filter = new IncrementalReleaseFilter(initialSize, cycleIncrement);
        final Table mf = m.where(filter);
        filter.start();
        return mf.sort("Tab", "Bucket", "Time");
    }

    @TearDown(Level.Invocation)
    public void teardown() throws Exception {
        ms.removeUpdateListener(listener);
        listener = null;
        ms.close();
        ms = null;
        ugp = null;
        engine.tearDown();
        engine = null;
    }

    @Benchmark
    @OperationsPerInvocation(50)
    public long updateCycle() {
        for (int i = 0; i < numCycles; ++i) {
            ugp.startCycleForUnitTests();
            filter.run();
            ugp.completeCycleForUnitTests();
        }
        System.out.println(listener.state);
        return listener.state;
    }
}
