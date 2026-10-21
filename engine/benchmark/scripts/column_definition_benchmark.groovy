//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//

// Console benchmark for table definition creation and validation in table operations. Run it in a Groovy console
// session (for example, ./gradlew server-jetty-app:run -Pgroovy) with enough heap for the chosen size.
//
// It builds a table of numColumns columns of varied types (scalars, arrays, and Vectors) with TableTools.newTable, so
// that setup involves no formula compilation, partitions it into numPartitions single-row constituents, then times
// partitioned table transforms over them:
//   identity: a baseline of transform overhead alone, which creates no new tables
//   flatten:  creates a new table, and so a new or reused definition, per constituent; flattening adds little else
//   sort:     the same, plus sorting
// Partitioning itself reuses the parent's definition for each constituent, so only the transforms create
// definitions.
//
// Compare builds of main, agent/column-definition-validation (which validates every definition it creates), and
// agent/column-definition-validation-perf (whose sort and flatten reuse the parent's definition instead). Compare the
// later runs of each transform: the first includes JIT compilation. Timings are wall-clock, as transforms may process
// constituents in parallel.

import io.deephaven.engine.table.impl.QueryTable
import io.deephaven.engine.table.impl.util.ColumnHolder
import io.deephaven.engine.util.TableTools
import io.deephaven.vector.DoubleVector
import io.deephaven.vector.DoubleVectorDirect
import io.deephaven.vector.IntVector
import io.deephaven.vector.IntVectorDirect
import io.deephaven.vector.ObjectVector
import io.deephaven.vector.ObjectVectorDirect

import java.time.Instant
import java.util.function.UnaryOperator

// Start small and scale up (10_000 -> 100_000 -> 1_000_000), watching the heap figure each run prints
def numPartitions = 100_000
def numColumns = 100
def runs = 3

// One data array per type, shared by every column of that type; one row per partition
final int n = numPartitions
final String[] strings = (0..<100).collect { "S$it".toString() } as String[]
final int[] keys = new int[n]
final int[] ints = new int[n]
final long[] longs = new long[n]
final double[] doubles = new double[n]
final float[] floats = new float[n]
final short[] shorts = new short[n]
final byte[] bytes = new byte[n]
final char[] chars = new char[n]
final Boolean[] flags = new Boolean[n]
final String[] texts = new String[n]
final Instant[] times = new Instant[n]
final int[][] intArrays = new int[n][]
final double[][] doubleArrays = new double[n][]
final String[][] stringArrays = new String[n][]
final IntVector[] intVectors = new IntVector[n]
final DoubleVector[] doubleVectors = new DoubleVector[n]
final ObjectVector[] stringVectors = new ObjectVector[n]
for (int i = 0; i < n; ++i) {
    keys[i] = i
    ints[i] = i % 1000
    longs[i] = i * 31L
    doubles[i] = i * 1.5d
    floats[i] = (float) ((i % 100) / 3.0d)
    shorts[i] = (short) (i % 100)
    bytes[i] = (byte) (i % 10)
    chars[i] = (char) (97 + i % 26)
    flags[i] = i % 2 == 0
    texts[i] = strings[i % 100]
    times[i] = Instant.ofEpochMilli(i)
    intArrays[i] = [i, i + 1] as int[]
    doubleArrays[i] = [i * 0.5d, i * 1.5d] as double[]
    stringArrays[i] = [strings[i % 100], "b"] as String[]
    intVectors[i] = new IntVectorDirect(i, i + 1)
    doubleVectors[i] = new DoubleVectorDirect(i * 0.5d, i * 1.5d)
    stringVectors[i] = new ObjectVectorDirect<String>([strings[i % 100], "b"] as String[])
}

// Column names and types cycle through these; names differ by type prefix and index (Int_0, Long_1, ...). Array and
// Vector columns are given explicit data and component types.
def columnTypes = [
        ["Int", { name -> TableTools.intCol(name, ints) }],
        ["Long", { name -> TableTools.longCol(name, longs) }],
        ["Double", { name -> TableTools.doubleCol(name, doubles) }],
        ["Float", { name -> TableTools.floatCol(name, floats) }],
        ["Short", { name -> TableTools.shortCol(name, shorts) }],
        ["Byte", { name -> TableTools.byteCol(name, bytes) }],
        ["Char", { name -> TableTools.charCol(name, chars) }],
        ["Flag", { name -> TableTools.booleanCol(name, flags) }],
        ["Text", { name -> TableTools.stringCol(name, texts) }],
        ["Time", { name -> TableTools.instantCol(name, times) }],
        ["Ints", { name -> new ColumnHolder<>(name, int[].class, int.class, false, intArrays) }],
        ["Doubles", { name -> new ColumnHolder<>(name, double[].class, double.class, false, doubleArrays) }],
        ["Texts", { name -> new ColumnHolder<>(name, String[].class, String.class, false, stringArrays) }],
        ["IntVec", { name -> new ColumnHolder<>(name, IntVector.class, int.class, false, intVectors) }],
        ["DblVec", { name -> new ColumnHolder<>(name, DoubleVector.class, double.class, false, doubleVectors) }],
        ["StrVec", { name -> new ColumnHolder<>(name, ObjectVector.class, String.class, false, stringVectors) }],
]
def holders = (0..<numColumns).collect { i ->
    def ct = columnTypes[i % columnTypes.size()]
    ct[1]("${ct[0]}_${i}".toString())
}
holders << TableTools.intCol("Key", keys)

def source = TableTools.newTable(holders as ColumnHolder[])
def pt = source.partitionBy("Key")
println "partitions: ${pt.table().size()}, columns per constituent: ${source.numColumns()}"

def ops = [
        identity: { t -> t },               // baseline: transform overhead only, no new tables
        flatten : { t -> t.flatten() },     // a new table per constituent, so a new (or reused) definition
        sort    : { t -> t.sort("Int_0") }, // the same, plus sorting
]

// Memoization would make repeated runs return cached constituents
def oldMemoize = QueryTable.setMemoizeResults(false)
try {
    ops.each { name, op ->
        runs.times { run ->
            System.gc()
            def start = System.nanoTime()
            def result = pt.transform(op as UnaryOperator)
            def ms = (System.nanoTime() - start) / 1_000_000d
            def rt = Runtime.getRuntime()
            printf("%-8s run %d: %,10.1f ms  %8.2f us/constituent  heap used %,d MB%n",
                    name, run, ms, ms * 1000 / numPartitions, (rt.totalMemory() - rt.freeMemory()) >> 20)
            result = null
        }
    }
} finally {
    QueryTable.setMemoizeResults(oldMemoize)
}
