//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.FileMetaData;

import java.util.Map;
import java.util.Optional;

public interface ParquetColumnResolver {

    interface Provider {
        ParquetColumnResolver init(FileMetaData metadata);
    }

    static ParquetColumnResolver of(Map<String, ColumnPath> map) {
        return new ParquetColumnResolverImpl(Map.copyOf(map));
    }

    Optional<ColumnPath> columnPath(String columnName);

    // todo: this is _post_ inference, we are not changing the inference logic...
    // do we _need_ to provide inference hooks?


}
