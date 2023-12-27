package io.deephaven.parquet.table;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value;

@Value.Immutable
@BuildableStyle
public abstract class S3ParquetInstructions {

    private final static int DEFAULT_MAX_CONCURRENT_REQUESTS = 20;
    private final static int DEFAULT_READ_AHEAD_COUNT = 1;
    private final static int DEFAULT_MAX_FRAGMENT_SIZE = 512 << 20; // 5 MB
    private final static int MIN_MAX_FRAGMENT_SIZE = 8 << 10; // 8 KB
    private final static int DEFAULT_MAX_CACHE_SIZE = 50;

    public static Builder builder() {
        return S3ParquetInstructions.builder();
    }

    public abstract String awsRegionName();

    @Value.Default
    public int maxConcurrentRequests() {
        return DEFAULT_MAX_CONCURRENT_REQUESTS;
    }

    @Value.Default
    public int readAheadCount() {
        return DEFAULT_READ_AHEAD_COUNT;
    }

    @Value.Default
    public int maxFragmentSize() {
        return DEFAULT_MAX_FRAGMENT_SIZE;
    }

    @Value.Default
    public int maxCacheSize() {
        return DEFAULT_MAX_CACHE_SIZE;
    }

    @Value.Check
    final void boundsCheckMaxConcurrentRequests() {
        if (maxConcurrentRequests() < 1) {
            throw new IllegalArgumentException("maxConcurrentRequests(=" + maxConcurrentRequests() + ") must be >= 1");
        }
    }

    @Value.Check
    final void boundsCheckReadAheadCount() {
        if (readAheadCount() < 0) {
            throw new IllegalArgumentException("readAheadCount(=" + readAheadCount() + ") must be >= 0");
        }
    }

    @Value.Check
    final void boundsCheckMaxFragmentSize() {
        if (maxFragmentSize() < MIN_MAX_FRAGMENT_SIZE) {
            throw new IllegalArgumentException("maxFragmentSize(=" + maxFragmentSize() + ") must be >= 8*1024 or 8 KB");
        }
    }

    @Value.Check
    final void boundsCheckMaxCacheSize() {
        if (maxCacheSize() < readAheadCount() + 1) {
            throw new IllegalArgumentException("maxCacheSize(=" + maxCacheSize() + ") must be >= 1 + readAheadCount");
        }
    }

    public interface Builder {
        Builder awsRegionName(String awsRegionName);

        Builder maxConcurrentRequests(int maxConcurrentRequests);

        Builder readAheadCount(int readAheadCount);

        Builder maxFragmentSize(int maxFragmentSize);

        Builder maxCacheSize(int maxCacheSize);

        S3ParquetInstructions build();
    }
}
