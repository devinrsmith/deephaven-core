//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.annotations.BuildableStyle;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.schema.MessageType;
import org.immutables.value.Value;

import java.util.Map;

/**
 * A mapping between Deephaven column names and Parquet {@link ColumnDescriptor column descriptors}.
 */
@Value.Immutable
@BuildableStyle
public abstract class ParquetColumnResolver {

    /**
     * {@link ParquetInstructions.Builder#setColumnResolverFactory(Factory)}
     */
    public interface Factory {

        /**
         *
         * @param metadata the Parquet file metadata
         * @return the Parquet column resolver
         */
        ParquetColumnResolver init(FileMetaData metadata);
    }

    public static Builder builder() {
        return ImmutableParquetColumnResolver.builder();
    }

    abstract MessageType schema();

    public abstract Map<String, ColumnDescriptor> mapping();

    @Value.Check
    final void checkColumns() {
        for (ColumnDescriptor columnDescriptor : mapping().values()) {
            if (!ParquetUtil.contains(schema(), columnDescriptor)) {
                throw new IllegalArgumentException("schema does not contain column descriptor " + columnDescriptor);
            }
        }
    }

    public interface Builder {

        Builder schema(MessageType schema);

        Builder putMapping(String key, ColumnDescriptor value);

        Builder putAllMapping(Map<String, ? extends ColumnDescriptor> entries);

        ParquetColumnResolver build();
    }
}
