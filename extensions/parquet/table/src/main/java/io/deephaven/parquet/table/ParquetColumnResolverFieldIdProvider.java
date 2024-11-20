//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.base.verify.Assert;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.TypeVisitor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The following is an example {@link ParquetColumnResolver.Provider} that may be useful for testing and debugging
 * purposes, but is not meant to be used for production use cases.
 */
public final class ParquetColumnResolverFieldIdProvider implements ParquetColumnResolver.Provider {

    /**
     *
     * @param columnNameToFieldId a map from Deephaven column names to field ids
     * @return the column resolver provider
     */
    public static ParquetColumnResolverFieldIdProvider of(Map<String, Integer> columnNameToFieldId) {
        return new ParquetColumnResolverFieldIdProvider(columnNameToFieldId
                .entrySet()
                .stream()
                .collect(Collectors.groupingBy(
                        Map.Entry::getValue,
                        Collectors.mapping(Map.Entry::getKey, Collectors.toSet()))));
    }

    private final Map<Integer, Set<String>> fieldIdsToDhColumnNames;

    private ParquetColumnResolverFieldIdProvider(Map<Integer, Set<String>> fieldIdsToDhColumnNames) {
        this.fieldIdsToDhColumnNames = Objects.requireNonNull(fieldIdsToDhColumnNames);
    }

    @Override
    public ParquetColumnResolver init(FileMetaData metadata) {
        final Map<ColumnPath, Type> pathToType = new HashMap<>();
        // This size estimate isn't correct if it's not a 1-to-1 mapping, but that is ok
        final Map<String, ColumnPath> nameToFieldIdPath = new HashMap<>(fieldIdsToDhColumnNames.size());
        // we don't want to include "schema" in the path, so _not_ concatenating metadata.getSchema().getName()
        metadata.getSchema().accept(new VisitorImpl(ColumnPath.get(), pathToType, nameToFieldIdPath));


        // This is an implementation detail that is tied to the implementation / RowGroupReader.getColumnChunk...
        // We need to pass the leaf paths to the column chunks.

        final Map<String, ColumnPath> nameToColumnChunkPath = new HashMap<>(nameToFieldIdPath.size());
//        for (Map.Entry<String, ColumnPath> e : nameToFieldIdPath.entrySet()) {
//            final ColumnPath path = e.getValue();
//            final Type type = Objects.requireNonNull(pathToType.get(path));
//            Type child = type;
//            while ((!child.isPrimitive()) && child.asGroupType().getFieldCount() == 1) {
//                child = child.asGroupType().getFields().get(0);
//            }
//        }

        final Map<String, ColumnDescriptor> nameToColumnDescriptor = new HashMap<>();
        // This could likely be implemented more efficiently by using a recursive type visitor, but it would take care
        // to make sure we are calculating ColumnDescriptor correctly.
        final List<ColumnDescriptor> columns = metadata.getSchema().getColumns();
        for (final ColumnDescriptor column : columns) {
            // Note: explicitly including length
            for (int i = 0; i <= column.getPath().length; i++) {
                final Type containingType = metadata.getSchema().getType(Arrays.copyOf(column.getPath(), i));
                if (containingType.getId() == null) {
                    continue;
                }
                final int fieldId = containingType.getId().intValue();
                final Set<String> set = fieldIdsToDhColumnNames.get(fieldId);
                if (set == null) {
                    continue;
                }
                for (String columnName : set) {
                    if (nameToColumnDescriptor.putIfAbsent(columnName, column) != null) {
                        throw new IllegalStateException(); // todo
                    }
                }
            }
        }





        return ParquetColumnResolver.of(nameToColumnChunkPath);
    }

    private static ColumnPath append(ColumnPath path, String part) {
        return ColumnPath.get(Stream.concat(Stream.of(path.toArray()), Stream.of(part)).toArray(String[]::new));
    }

    private class VisitorImpl implements TypeVisitor {
        private final ColumnPath path;
        private final Map<ColumnPath, Type> pathToType;
        private final Map<String, ColumnPath> nameToPath;

        private VisitorImpl(ColumnPath path, Map<ColumnPath, Type> pathToType, Map<String, ColumnPath> nameToPath) {
            this.path = Objects.requireNonNull(path);
            this.pathToType = Objects.requireNonNull(pathToType);
            this.nameToPath = Objects.requireNonNull(nameToPath);
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
                field.accept(new VisitorImpl(append(path, field.getName()), pathToType, nameToPath));
            }
        }

        private void handleType(Type type) {
            Assert.eqNull(pathToType.put(path, type), "pathToType.put(path, type)");
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
                final ColumnPath existingPath = nameToPath.putIfAbsent(deephavenColumnName, path);
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

    private static boolean isStandardList(Type t) {
        if (!isStandardListOuter(t)) {
            return false;
        }
        t = t.asGroupType().getFields().get(0);
        if (!isStandardListMiddle(t)) {
            return false;
        }
        t = t.asGroupType().getFields().get(0);
        return isStandardListInner(t);
    }

    private static boolean isStandardListOuter(Type t) {
        return !t.isPrimitive()
                && (t.getRepetition() == Type.Repetition.REQUIRED || t.getRepetition() == Type.Repetition.OPTIONAL)
                && LogicalTypeAnnotation.listType().equals(t.getLogicalTypeAnnotation())
                && t.asGroupType().getFieldCount() == 1;
    }

    private static boolean isStandardListMiddle(Type t) {
        // not checking name is "list"
        // > However, these names may not be used in existing data and should not be enforced as errors when reading
        return !t.isPrimitive()
                && t.getRepetition() == Type.Repetition.REPEATED
                && t.asGroupType().getFieldCount() == 1;
    }

    private static boolean isStandardListInner(Type t) {
        // not checking name is "element"
        // > However, these names may not be used in existing data and should not be enforced as errors when reading
        return t.getRepetition() == Type.Repetition.REQUIRED || t.getRepetition() == Type.Repetition.OPTIONAL;
    }

    interface ListType {

        GroupType listType();

        Type elementType();

        String[] elementPath();

        default boolean listNullable() {
            return listType().getRepetition() == Type.Repetition.OPTIONAL;
        }

        default boolean elementNullable() {
            return elementType().getRepetition() == Type.Repetition.OPTIONAL;
        }
    }

    final static class StandardList implements ListType {
        private final GroupType listType;

        private StandardList(GroupType listType) {
            this.listType = Objects.requireNonNull(listType);
        }

        @Override
        public GroupType listType() {
            return listType;
        }

        @Override
        public String[] elementPath() {
            return new String[] { middleType().getName(), elementType().getName() };
        }

        @Override
        public Type elementType() {
            return middleType().getFields().get(0);
        }

        private GroupType middleType() {
            return listType.getFields().get(0).asGroupType();
        }
    }

    private static class ListType2 {
        private final GroupType listType;
        private final Type elementType;
        private final String[] elementPath;
        private final boolean nullableList;
        private final boolean nullableElements;

        public ListType(GroupType listType, Type elementType, String[] elementPath, boolean nullableList,
                boolean nullableElements) {
            this.listType = Objects.requireNonNull(listType);
            this.elementType = Objects.requireNonNull(elementType);
            this.elementPath = Objects.requireNonNull(elementPath);
            this.nullableList = nullableList;
            this.nullableElements = nullableElements;
        }
    }

}
