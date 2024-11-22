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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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
        final FieldIdMappingVisitor visitor = new FieldIdMappingVisitor();
        ParquetUtil.walk(metadata.getSchema(), visitor);
        return ParquetColumnResolver.of(metadata.getSchema(), visitor.nameToColumnDescriptor);
    }

    private class FieldIdMappingVisitor implements ParquetUtil.Visitor {
        private final Map<String, ColumnDescriptor> nameToColumnDescriptor = new HashMap<>();

        @Override
        public void accept(Collection<Type> path, PrimitiveType primitiveType) {
            // There are different resolution strategies that could all be reasonable. We could consider using only the
            // field id closest to the leaf. This version, however, takes the most general approach and considers field
            // ids wherever they appear; ultimately, only being resolvable if the field id mapping is unambiguous.
            for (Type type : path) {
                if (type.getId() == null) {
                    continue;
                }
                final int fieldId = type.getId().intValue();
                final Set<String> set = fieldIdsToDhColumnNames.get(fieldId);
                if (set == null) {
                    continue;
                }
                final ColumnDescriptor columnDescriptor = ParquetUtil.makeColumnDescriptor(path, primitiveType);
                for (String columnName : set) {
                    if (nameToColumnDescriptor.putIfAbsent(columnName, columnDescriptor) != null) {
                        throw new IllegalStateException(); // todo
                    }
                }
            }
        }
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

}
