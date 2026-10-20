//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//

// Console benchmark for table definition creation and validation in table operations. Run it in a Groovy console
// session (for example, ./gradlew server-jetty-app:run -Pgroovy) with enough heap for the chosen size.
//
// It partitions a table of numColumns columns of varied types into numPartitions single-row constituents, then times
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
import io.deephaven.engine.util.TableTools
import java.util.function.UnaryOperator

// Start small and scale up (10_000 -> 100_000 -> 1_000_000), watching the heap figure each run prints
def numPartitions = 100_000
def numColumns = 100
def runs = 3

// Column names and types cycle through these; names differ by type prefix and index (Int_0, Long_1, ...)
def typeFormulas = [
        ["Int", "(int) (ii % 1000)"],
        ["Long", "ii * 31"],
        ["Double", "ii * 1.5"],
        ["Float", "(float) ((ii % 100) / 3.0)"],
        ["Short", "(short) (ii % 100)"],
        ["Byte", "(byte) (ii % 10)"],
        ["Char", "(char) (97 + ii % 26)"],
        ["Flag", "ii % 2 == 0"],
        ["Text", "`S` + (ii % 100)"],
        ["Time", "epochNanosToInstant(ii * 1000000L)"],
]
def formulas = (0..<numColumns).collect { i ->
    def tf = typeFormulas[i % typeFormulas.size()]
    "${tf[0]}_${i} = ${tf[1]}".toString()
}
formulas << "Key = (int) ii" // one row per partition

def source = TableTools.emptyTable(numPartitions).update(formulas as String[])
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
