package io.deephaven.parquet.base;

import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.immutables.value.Value;

@Value.Immutable
public abstract class FileMetadata {

    static FileMetadata of(FileMetaData fileMetadata) {
        return null;
    }

    @Value.Parameter
    public abstract FileMetaData fileMetadata();

    @Value.Derived
    public Schema schema() {
        return Schema.of(fileMetadata().getSchema());
    }
}
