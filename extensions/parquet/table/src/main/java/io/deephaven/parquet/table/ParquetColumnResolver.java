//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.schema.MessageType;

import java.util.Map;
import java.util.Optional;

/**
 * Resolves Deephaven column namen into Parquet ColumnPaths.
 */
public interface ParquetColumnResolver {

    /**
     * {@link ParquetInstructions.Builder#setColumnResolverProvider(Provider)}
     */
    interface Provider {

        /**
         *
         * @param metadata the Parquet file metadata
         * @return the Parquet column resolver
         */
        ParquetColumnResolver init(FileMetaData metadata);
    }

    /**
     * Creates a parquet column resolver from a {@link Map#copyOf(Map) copy of} {@code map}.
     *
     * @param map the map
     * @return the Parquet column resolver
     */
    static ParquetColumnResolver of(MessageType schema, Map<String, ColumnDescriptor> map) {
        return null; // new ParquetColumnResolverFixedImpl(Map.copyOf(map));
    }

    MessageType schema();

    /**
     * Returns the column path for a given {@code columnName} if present.
     *
     * @param columnName the Deephaven column name
     * @return the column path, if mapped
     */
    Optional<ColumnDescriptor> columnDescriptor(String columnName);
}
