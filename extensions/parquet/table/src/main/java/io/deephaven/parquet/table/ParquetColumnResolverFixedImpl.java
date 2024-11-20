//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.FileMetaData;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

final class ParquetColumnResolverFixedImpl implements ParquetColumnResolver {

    private final FileMetaData metadata;
    private final Map<String, ColumnDescriptor> map;

    ParquetColumnResolverFixedImpl(FileMetaData metadata, Map<String, ColumnDescriptor> map) {
        this.metadata = metadata;
        this.map = Objects.requireNonNull(map);
    }

    @Override
    public FileMetaData schema() {
        return metadata;
    }

    @Override
    public Optional<ColumnDescriptor> columnDescriptor(String columnName) {
        return Optional.ofNullable(map.get(columnName));
    }
}
