import io.deephaven.db.v2.sources.immutable.ImmutableIntArraySourceDiskLmdbChunk

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