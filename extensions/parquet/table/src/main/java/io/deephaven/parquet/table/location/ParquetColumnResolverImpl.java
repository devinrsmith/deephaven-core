//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import org.apache.parquet.hadoop.metadata.ColumnPath;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

final class ParquetColumnResolverImpl implements ParquetColumnResolver {

    private final Map<String, ColumnPath> map;

    ParquetColumnResolverImpl(Map<String, ColumnPath> map) {
        this.map = Objects.requireNonNull(map);
    }

    @Override
    public Optional<ColumnPath> columnPath(String columnName) {
        return Optional.ofNullable(map.get(columnName));
    }
}
