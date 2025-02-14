//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import com.google.common.collect.AbstractIterator;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.parquet.table.location.ParquetColumnResolver;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

final class ResolverFactory implements ParquetColumnResolver.Factory {

    private final DefinitionInstructions instructions;

    ResolverFactory(DefinitionInstructions instructions) {
        this.instructions = Objects.requireNonNull(instructions);
    }

    @Override
    public ParquetColumnResolver of(TableKey tableKey, ParquetTableLocationKey tableLocationKey) {
        // final IcebergTableParquetLocationKey itplk = (IcebergTableParquetLocationKey) tableLocationKey;
        // TODO: we should be able to get the writtenSchema for this location to enhance our error messages
        return new Resolver(tableLocationKey.getSchema());
    }

    private Optional<List<Types.NestedField>> fieldPath(String columnName) {
        final ColumnInstructions ci = instructions.columnInstructions().get(columnName);
        return ci == null
                ? Optional.empty()
                : Optional.of(ci.path().resolve(instructions.schema()));
    }

    private class Resolver implements ParquetColumnResolver {

        // private final Schema writtenSchema;
        private final MessageType parquetSchema;

        private Resolver(MessageType parquetSchema) {
            this.parquetSchema = Objects.requireNonNull(parquetSchema);
        }

        @Override
        public Optional<List<String>> of(String columnName) {
            final List<Types.NestedField> fields = fieldPath(columnName).orElse(null);
            if (fields == null) {
                // DH did not map this columnName
                return Optional.empty();
            }
            try {
                return Optional.of(adapt(fields.iterator())
                        .map(Type::getName)
                        .collect(Collectors.toList()));
            } catch (RuntimeException e) {
                if (e.getCause() instanceof MappingException) {
                    // https://lists.apache.org/thread/88md2fdk17k26cl4gj3sz6sdbtwcgbk5
                    // We can improve the state of our errors here if we incorporate the "writtenSchema"; for example,
                    // we should know apriori if a field has been deleted

                    // we don't have a good way to communicate this state to the upper level;
                    // in the case of a NotFound, it's possible the
                    // throw e; // todo: don't do this
                    return Optional.empty();
                } else {
                    throw e;
                }
            }
        }

        private Stream<Type> adapt(Iterator<Types.NestedField> it) {
            return StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(new It2(parquetSchema, it),
                            Spliterator.ORDERED | Spliterator.NONNULL),
                    false);
        }
    }

    private static class It2 extends AbstractIterator<Type> {
        private final Iterator<Types.NestedField> fieldIt;
        private Iterator<Type> nextIt; // may be up to 3 in length
        private Type current;

        public It2(MessageType schema, Iterator<Types.NestedField> fieldIt) {
            this.fieldIt = Objects.requireNonNull(fieldIt);
            this.current = Objects.requireNonNull(schema);
            this.nextIt = Collections.emptyIterator(); // TODO: VALIDATE
        }

        @Override
        protected @Nullable Type computeNext() {
            if (nextIt.hasNext()) {
                return current = Objects.requireNonNull(nextIt.next());
            }
            if (!fieldIt.hasNext()) {
                current = null;
                return endOfData();
            }
            final Types.NestedField field = fieldIt.next();
            final List<Type> next;
            try {
                next = find(current.asGroupType(), field);
            } catch (MappingException e) {
                throw new RuntimeException(e);
            }
            if (next.isEmpty()) {
                throw new IllegalStateException();
            }
            // usually empty
            nextIt = next.subList(1, next.size()).iterator();
            return current = Objects.requireNonNull(next.get(0));
        }
    }

    private static List<Type> find(GroupType type, Types.NestedField field) throws MappingException {
        org.apache.iceberg.types.Type icebergType = field.type();
        if (icebergType.isPrimitiveType() || icebergType.isStructType()) {
            return List.of(findOne(type, field.fieldId()));
        }
        if (icebergType.isMapType()) {
            throw new MapUnsupported();
        }
        if (icebergType.isListType()) {
            throw new ListUnsupported();
        }
        throw new IllegalStateException();
    }

    private static Type findOne(GroupType type, int fieldId) throws MappingException {
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
