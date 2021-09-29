import io.deephaven.db.v2.sources.immutable.ImmutableIntArraySourceDisk
import io.deephaven.db.v2.sources.immutable.ImmutableIntArraySourceDiskLmdbChunk
import io.deephaven.db.tables.utils.ParquetTools
import java.nio.file.Paths

nanoStart = System.nanoTime()
view500m = emptyTable(500000000).view("X=i").sumBy()
nanoDuration = System.nanoTime() - nanoStart
nanosPerOpsView500m = nanoDuration / 500000000
println(nanosPerOpsView500m + " nanos / op (500m view)")

nanoStart = System.nanoTime()
parquet500m = ParquetTools.readTable("/data/500mm.parquet").sumBy()
nanoDuration = System.nanoTime() - nanoStart
nanosPerOpsParquet500m = nanoDuration / 500000000
println(nanosPerOpsParquet500m + " nanos / op (500m parquet)")

nanoStart = System.nanoTime()
memmap500m = ImmutableIntArraySourceDisk.table(Paths.get("/data/500m.bin")).sumBy()
nanoDuration = System.nanoTime() - nanoStart
nanosPerOpsMemmap500m = nanoDuration / 500000000
println(nanosPerOpsMemmap500m + " nanos / op (500m memmap)")

nanoStart = System.nanoTime()
lmdbChunk500m = ImmutableIntArraySourceDiskLmdbChunk.tableSum(Paths.get("/data/500m.lmdb.chunk"), 500000000)
nanoDuration = System.nanoTime() - nanoStart
nanosPerOpsLmdbChunk500m = nanoDuration / 500000000
println(nanosPerOpsLmdbChunk500m + " nanos / op (500m lmdb chunk)")