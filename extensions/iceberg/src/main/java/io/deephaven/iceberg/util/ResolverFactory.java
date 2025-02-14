//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.iceberg.internal.SchemaHelper;
import io.deephaven.iceberg.location.IcebergTableParquetLocationKey;
import io.deephaven.parquet.table.location.ParquetColumnResolver;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

final class ResolverFactory implements ParquetColumnResolver.Factory {

    private final DefinitionInstructions instructions;
    private final boolean raiseErrorOnUnexpectedMissingData;

    ResolverFactory(DefinitionInstructions instructions, boolean raiseErrorOnUnexpectedMappingError) {
        this.instructions = Objects.requireNonNull(instructions);
        this.raiseErrorOnUnexpectedMissingData = raiseErrorOnUnexpectedMappingError;
    }

    @Override
    public ParquetColumnResolver of(TableKey tableKey, ParquetTableLocationKey tableLocationKey) {
        return new Resolver((IcebergTableParquetLocationKey) tableLocationKey);
    }

    private class Resolver implements ParquetColumnResolver {

        private final IcebergTableParquetLocationKey key;

        public Resolver(IcebergTableParquetLocationKey key) {
            this.key = Objects.requireNonNull(key);
        }

        MessageType parquetSchema() {
            // Note: intentionally delaying the reading of this until we actually want to resolve it
            return key.getSchema();
        }

        Schema icebergSchema() {
            // the schema used to write the manifest / datafiles
            return key.manifestSchema();
        }

        @Override
        public Optional<List<String>> of(String columnName) {
            final ColumnInstructions ci = instructions.columnInstructions().get(columnName);
            if (ci == null) {
                // DH did not map this column name
                return Optional.empty();
            }
            final List<Types.NestedField> fields;
            try {
                fields = ci.path().resolve(icebergSchema());
            } catch (SchemaHelper.PathException e) {
                // The written file does not have this column
                return Optional.empty();
            }
            try {
                return Optional.of(resolve(parquetSchema(), fields));
            } catch (MappingException e) {

                // is field required?

                if (raiseErrorOnUnexpectedMissingData) {
                    throw new RuntimeException(String.format("Unexpected mapping error for column `%s` in key=%s", columnName, key), e);
                }
                // todo: log?
                return Optional.empty();
            }
        }
    }

    private static List<String> resolve(MessageType schema, List<Types.NestedField> fields) throws MappingException {
        Type current = schema;
        final List<String> out = new ArrayList<>();
        for (final Types.NestedField field : fields) {
            final List<Type> types = find(current.asGroupType(), field);
            for (Type type : types) {
                out.add(type.getName());
            }
            current = types.get(types.size() - 1);
        }
        return out;
    }

    private static List<Type> find(GroupType type, Types.NestedField field) throws MappingException {
        final int fieldId = field.fieldId();
        final org.apache.iceberg.types.Type icebergType = field.type();
        if (icebergType.isPrimitiveType()) {
            return List.of(findPrimitive(fieldId, type, icebergType.asPrimitiveType()));
        }
        if (icebergType.isStructType()) {
            return List.of(findStruct(fieldId, type, icebergType.asStructType()));
        }
        if (icebergType.isMapType()) {
            return findMap(fieldId, type, icebergType.asMapType());
        }
        if (icebergType.isListType()) {
            return findList(fieldId, type, icebergType.asListType());
        }
        throw new IllegalStateException();
    }

    private static Type findField(int fieldId, GroupType type) throws MappingException {
        Type found = null;
        for (Type field : type.getFields()) {
            if (field.getId() != null && field.getId().intValue() == fieldId) {
                if (found != null) {
                    throw new Duplicate();
                }
                found = field;
            }
        }
        if (found == null) {
            throw new NotFound("not found " + fieldId);
        }
        return found;
    }

    private static Type findPrimitive(int fieldId, GroupType type, org.apache.iceberg.types.Type.PrimitiveType icebergType) throws MappingException {
        final Type found = findField(fieldId, type);
        checkCompatible(found, icebergType);
        return found;
    }

    private static Type findStruct(int fieldId, GroupType type, Types.StructType structType) throws MappingException {
        final Type found = findField(fieldId, type);
        checkCompatible(found, structType);
        return found;
    }

    private static List<Type> findMap(int fieldId, GroupType type, Types.MapType itype) throws MappingException {
        throw new MapUnsupported();
    }

    private static List<Type> findList(int fieldId, GroupType type, Types.ListType itype) throws MappingException {
        throw new ListUnsupported();
    }

    private static void checkCompatible(Type ptype, org.apache.iceberg.types.Type.PrimitiveType itype) {
        // TODO
    }

    private static void checkCompatible(Type ptype, Types.StructType itype) {
        // TODO
    }

    private static void checkCompatible(List<Type> ptypes, Types.ListType itype) {

    }

    private static void checkCompatible(List<Type> ptypes, Types.MapType itype) {

    }

    private static abstract class MappingException extends Exception {

        public MappingException() {}

        public MappingException(String message) {
            super(message);
        }
    }

    private static class NotFound extends MappingException {

        public NotFound(String message) {
            super(message);
        }
    }

    private static class Duplicate extends MappingException {

    }

    private static class MapUnsupported extends MappingException {

    }

    private static class ListUnsupported extends MappingException {

    }
}
