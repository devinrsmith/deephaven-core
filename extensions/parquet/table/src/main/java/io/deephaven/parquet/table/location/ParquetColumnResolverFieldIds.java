//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.TypeVisitor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * The following is an example {@link ParquetColumnResolver.Provider} that may be useful for testing and debugging
 * purposes, but is not meant to be used for production use cases.
 */
public final class ParquetColumnResolverFieldIds implements ParquetColumnResolver.Provider {


    /**
     *
     * @param columnNameToFieldId a map from Deephaven column names to field ids
     * @return the column resolver provider
     */
    public static ParquetColumnResolverFieldIds of(Map<String, Integer> columnNameToFieldId) {
        final Map<Integer, Set<String>> inverse = new HashMap<>(columnNameToFieldId.size());
        for (Map.Entry<String, Integer> e : columnNameToFieldId.entrySet()) {
            final Set<String> set = inverse.computeIfAbsent(e.getValue(), id -> new HashSet<>());
            set.add(e.getKey());
        }
        return new ParquetColumnResolverFieldIds(inverse);
    }

    private final Map<Integer, Set<String>> fieldIdsToDhColumnNames;

    private ParquetColumnResolverFieldIds(Map<Integer, Set<String>> fieldIdsToDhColumnNames) {
        this.fieldIdsToDhColumnNames = Objects.requireNonNull(fieldIdsToDhColumnNames);
    }

    @Override
    public ParquetColumnResolver init(FileMetaData metadata) {
        // This size estimate isn't correct if it's not a 1-to-1 mapping, but that is ok
        final Map<String, ColumnPath> map = new HashMap<>(fieldIdsToDhColumnNames.size());
        // we don't want to include "schema" in the path, so _not_ concatenating metadata.getSchema().getName()
        metadata.getSchema().accept(new VisitorImpl(ColumnPath.get(), map));
        return ParquetColumnResolver.of(map);
    }

    private static ColumnPath append(ColumnPath path, String part) {
        return ColumnPath.get(Stream.concat(Stream.of(path.toArray()), Stream.of(part)).toArray(String[]::new));
    }

    private void visit(Type type, ColumnPath path, Map<String, ColumnPath> map) {
        type.accept(new VisitorImpl(append(path, type.getName()), map));
    }

    private class VisitorImpl implements TypeVisitor {
        private final ColumnPath path;
        private final Map<String, ColumnPath> map;

        private VisitorImpl(ColumnPath path, Map<String, ColumnPath> map) {
            this.path = Objects.requireNonNull(path);
            this.map = Objects.requireNonNull(map);
        }

        @Override
        public void visit(MessageType messageType) {
            handleGroupType(messageType);
        }

        @Override
        public void visit(GroupType groupType) {
            handleGroupType(groupType);
        }

        @Override
        public void visit(PrimitiveType primitiveType) {
            handleType(primitiveType);
        }

        private void handleGroupType(GroupType messageType) {
            handleType(messageType);
            for (Type field : messageType.getFields()) {
                ParquetColumnResolverFieldIds.this.visit(field, path, map);
            }
        }

        private void handleType(Type type) {
            final Type.ID id = type.getId();
            if (id == null) {
                return;
            }
            final int fieldId = id.intValue();
            final Set<String> deephavenColumnNames = fieldIdsToDhColumnNames.get(fieldId);
            if (deephavenColumnNames == null) {
                return;
            }
            for (final String deephavenColumnName : deephavenColumnNames) {
                final ColumnPath existingPath = map.putIfAbsent(deephavenColumnName, path);
                // Even though we know that based on the user input (Map<String, Integer> fieldIds) that the column
                // names are unique, it's possible that the parquet file has multiple columns with the same field id.
                // This is _not_ an issue with the Parquet file, as they don't provide any guarantees about the field
                // ids. This is also only an issue if DH cares about the column.
                if (existingPath != null) {
                    throw new IllegalStateException(String.format(
                            "Parquet schema has multiple paths with the same field id. Basic field id resolution is not possible. columnName=%s, fieldId=%d, path=%s, path=%s",
                            deephavenColumnName, fieldId, existingPath, path));
                }
            }
        }
    }
}
