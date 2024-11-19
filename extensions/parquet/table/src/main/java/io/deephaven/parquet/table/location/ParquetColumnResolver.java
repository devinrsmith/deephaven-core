//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.FileMetaData;

import java.util.Map;
import java.util.Optional;

/**
 *
 */
public interface ParquetColumnResolver {

    /**
     *
     */
    interface Provider {
        ParquetColumnResolver init(FileMetaData metadata);
    }

    /**
     * Creates a parquet column resolver from a {@link Map#copyOf(Map) copy of} {@code map}.
     *
     * @param map the map
     * @return the resolver
     */
    static ParquetColumnResolver of(Map<String, ColumnPath> map) {
        return new ParquetColumnResolverImpl(Map.copyOf(map));
    }

    /**
     * Returns the column path for a given {@code columnName} if mapped.
     *
     * @param columnName the Deephaven column name
     * @return the column path, if mapped
     */
    Optional<ColumnPath> columnPath(String columnName);
}
