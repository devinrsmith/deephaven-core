//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.FileMetaData;

public interface ParquetColumnResolver {

    interface Provider {
        ParquetColumnResolver init(FileMetaData metadata);
    }

    ColumnPath columnPath(String columnName);
}
