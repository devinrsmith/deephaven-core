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
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public final class ParquetColumnResolverFieldIds implements ParquetColumnResolver.Provider {

    public static ParquetColumnResolverFieldIds of(Map<String, Integer> fieldIds) {
        return new ParquetColumnResolverFieldIds(new HashMap<>(fieldIds));
    }

    private final Map<String, Integer> fieldIds;

    private ParquetColumnResolverFieldIds(Map<String, Integer> fieldIds) {
        this.fieldIds = Objects.requireNonNull(fieldIds);
    }

    @Override
    public ParquetColumnResolver init(FileMetaData metadata) {
        final Map<String, ColumnPath> map = new HashMap<>(fieldIds.size());
        visit(metadata.getSchema(), new String[0], map);
        return new ResolverImpl(map);
    }

    private static class ResolverImpl implements ParquetColumnResolver {
        private final Map<String, ColumnPath> map;

        private ResolverImpl(Map<String, ColumnPath> map) {
            this.map = Objects.requireNonNull(map);
        }

        @Override
        public ColumnPath columnPath(String columnName) {
            final ColumnPath path = map.get(columnName);
            return path == null
                    ? ColumnPath.get(columnName)
                    : path;
        }
    }

    private void visit(Type type, String[] path, Map<String, ColumnPath> map) {
        final String[] nextPath = Stream.concat(Stream.of(path), Stream.of(type.getName())).toArray(String[]::new);
        type.accept(new VisitorImpl(nextPath, map));
    }

    private class VisitorImpl implements TypeVisitor {
        private final String[] path;
        private final Map<String, ColumnPath> map;

        private VisitorImpl(String[] path, Map<String, ColumnPath> map) {
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
            for (final Map.Entry<String, Integer> e : fieldIds.entrySet()) {
                if (e.getValue() == fieldId) {
                    final String deephavenColumnName = e.getKey();
                    final ColumnPath newPath = ColumnPath.get(path);
                    final ColumnPath existingPath = map.putIfAbsent(deephavenColumnName, newPath);
                    if (existingPath != null) {
                        throw new IllegalStateException(String.format(
                                "Parquet schema has multiple paths with the same field id. Basic field id resolution is not possible. field_id=%d, %s / %s",
                                fieldId, existingPath, newPath));
                    }
                }
            }
        }
    }
}
