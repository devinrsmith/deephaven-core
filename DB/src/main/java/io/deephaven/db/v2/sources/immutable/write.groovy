import io.deephaven.db.tables.utils.TableTools
import io.deephaven.db.v2.sources.immutable.ImmutableIntArraySourceDisk
import io.deephaven.db.v2.sources.immutable.ImmutableIntArraySourceDiskLmdbChunk
import io.deephaven.db.tables.utils.ParquetTools
import java.nio.file.Paths

int amount = 100000000

// 22.098690248 nanos / write op (500m parquet)
// nanoStart = System.nanoTime()
// ParquetTools.writeTable(TableTools.emptyTable(amount).view("I=i"), "/data/500m.parquet")
// println((System.nanoTime() - nanoStart) / amount + " nanos / write op (500m parquet)")

// 12.335783758 nanos / write op (500m memmap)
// nanoStart = System.nanoTime()
// ImmutableIntArraySourceDisk.write(Paths.get("/data/500m.mmap"), amount)
// println((System.nanoTime() - nanoStart) / amount + " nanos / write op (500m memmap)")

// 24.920858182 nanos / write op (500m lmdb.chunk.1)
// nanoStart = System.nanoTime()
// ImmutableIntArraySourceDiskLmdbChunk.write1(Paths.get("/data/500m.lmdb.chunk.1"), amount)
// println((System.nanoTime() - nanoStart) / amount + " nanos / write op (500m lmdb.chunk.1)")



nanoStart = System.nanoTime()
ImmutableIntArraySourceDiskLmdbChunk.write2(Paths.get("/data/1m.lmdb.chunk.real"), 1000000)
println((System.nanoTime() - nanoStart) / 1000000 + " nanos / write op (1m lmdb.chunk.real)")

nanoStart = System.nanoTime()
ImmutableIntArraySourceDiskLmdbChunk.write2(Paths.get("/data/2m.lmdb.chunk.real"), 2000000)
println((System.nanoTime() - nanoStart) / 2000000 + " nanos / write op (2m lmdb.chunk.real)")

nanoStart = System.nanoTime()
ImmutableIntArraySourceDiskLmdbChunk.write2(Paths.get("/data/4m.lmdb.chunk.real"), 4000000)
println((System.nanoTime() - nanoStart) / 4000000 + " nanos / write op (4m lmdb.chunk.real)")

nanoStart = System.nanoTime()
ImmutableIntArraySourceDiskLmdbChunk.write2(Paths.get("/data/8m.lmdb.chunk.real"), 8000000)
println((System.nanoTime() - nanoStart) / 8000000 + " nanos / write op (8m lmdb.chunk.real)")

nanoStart = System.nanoTime()
ImmutableIntArraySourceDiskLmdbChunk.write2(Paths.get("/data/16m.lmdb.chunk.real"), 16000000)
println((System.nanoTime() - nanoStart) / 16000000 + " nanos / write op (16m lmdb.chunk.real)")

nanoStart = System.nanoTime()
ImmutableIntArraySourceDiskLmdbChunk.write2(Paths.get("/data/32m.lmdb.chunk.real"), 32000000)
println((System.nanoTime() - nanoStart) / 32000000 + " nanos / write op (32m lmdb.chunk.real)")

nanoStart = System.nanoTime()
ImmutableIntArraySourceDiskLmdbChunk.write2(Paths.get("/data/500m.lmdb.chunk.real"), 500000000)
println((System.nanoTime() - nanoStart) / 500000000 + " nanos / write op (500m lmdb.chunk.real)")


nanoStart = System.nanoTime()
ImmutableIntArraySourceDiskLmdbChunk.write2(500000000, 128)
println((System.nanoTime() - nanoStart) / 500000000 + " nanos / write op (500m & 128)")

nanoStart = System.nanoTime()
ImmutableIntArraySourceDiskLmdbChunk.write2(500000000, 256)
println((System.nanoTime() - nanoStart) / 500000000 + " nanos / write op (500m & 256)")

nanoStart = System.nanoTime()
ImmutableIntArraySourceDiskLmdbChunk.write2(500000000, 512)
println((System.nanoTime() - nanoStart) / 500000000 + " nanos / write op (500m & 512)")

nanoStart = System.nanoTime()
ImmutableIntArraySourceDiskLmdbChunk.write2(500000000, 1024)
println((System.nanoTime() - nanoStart) / 500000000 + " nanos / write op (500m & 1024)")

nanoStart = System.nanoTime()
ImmutableIntArraySourceDiskLmdbChunk.write2(500000000, 2048)
println((System.nanoTime() - nanoStart) / 500000000 + " nanos / write op (500m & 2048)")