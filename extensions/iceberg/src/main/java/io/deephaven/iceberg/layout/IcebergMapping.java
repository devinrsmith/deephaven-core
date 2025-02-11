package io.deephaven.iceberg.layout;

import com.google.common.collect.AbstractIterator;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.iceberg.base.IcebergUtils;
import io.deephaven.parquet.table.location.ParquetColumnResolver;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Value.Immutable
@BuildableStyle
public abstract class IcebergMapping {

    public static Builder builder() {
        return ImmutableIcebergMapping.builder();
    }

    public static IcebergMapping infer(Schema schema) {
        // todo: option to throw error on unmappable fields
        // todo: map key/value could be "key" / "value"?
        // similar to io.deephaven.iceberg.util.IcebergTableAdapter.fromSchema
        final Set<String> usedNames = new HashSet<>();
        final List<ColumnDefinition<?>> columnDefinitions = new ArrayList<>();
        Builder builder = IcebergMapping.builder();
        for (final Types.NestedField column : schema.columns()) {
            if (column.type().isPrimitiveType()) {
                final String dhName = NameValidator.legalizeColumnName(column.name(), usedNames);
                usedNames.add(dhName);
                builder.putMap(dhName, new int[] { column.fieldId() });
                columnDefinitions.add(ColumnDefinition.of(dhName, IcebergUtils.convertToDHType(column.type())));
            }
        }
        return builder
                .schema(schema)
                .definition(TableDefinition.of(columnDefinitions))
                .build();
    }

    public abstract Schema schema();

    public abstract TableDefinition definition();

    // does not support mapping to a key / value of a map type
    abstract Map<String, int[]> map(); // todo: verify this is pointing to a schema leaf; arguably, doesn't have to be a leaf if it's a list? b/c really, the list type is probably better than a path to the elemnt

    public interface Builder {

        Builder definition(TableDefinition definition);

        Builder schema(Schema schema);

        Builder putMap(String key, int[] value);

        IcebergMapping build();
    }

    final Optional<List<Types.NestedField>> fieldPath(String columnName) {
        final int[] schemaPath = map().get(columnName);
        return schemaPath == null
                ? Optional.empty()
                : Optional.of(SchemaHelper.nestedFields(schema(), schemaPath));
    }

    public final ParquetColumnResolver.Factory columnResolverFactory() {
        return new ResolverFactory();
    }

    private class ResolverFactory implements ParquetColumnResolver.Factory {
        @Override
        public ParquetColumnResolver of(TableKey tableKey, ParquetTableLocationKey tableLocationKey) {
            //final IcebergTableParquetLocationKey itplk = (IcebergTableParquetLocationKey) tableLocationKey;
            // TODO: we should be able to get the writtenSchema for this location to enhance our error messages
            return new Resolver(tableLocationKey.getSchema());
        }
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
            List<String> parquetPath = null;
            try {
                parquetPath = adapt(fields.iterator())
                        .map(Type::getName)
                        .collect(Collectors.toList());
            } catch (RuntimeException e) {
                if (e.getCause() instanceof MappingException) {
                    // We can improve the state of our errors here if we incorporate the "writtenSchema"; for example,
                    // we should know apriori if a field has been deleted

                    // we don't have a good way to communicate this state to the upper level;
                    // in the case of a NotFound, it's possible the
                    throw e; // todo: don't do this
                } else {
                    throw e;
                }
            }
            return Optional.of(parquetPath);
        }

        private Stream<Type> adapt(Iterator<Types.NestedField> it) {
            return StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(new It2(parquetSchema, it), Spliterator.ORDERED | Spliterator.NONNULL),
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
            throw new NotFound();
        }
        return found;
    }

    private static abstract class MappingException extends Exception {

    }

    private static class NotFound extends MappingException {

    }

    private static class Duplicate extends MappingException {

    }

    private static class MapUnsupported extends MappingException {

    }

    private static class ListUnsupported extends MappingException {

    }

    private static class IcebergVisitor {

        private Builder builder;

        private List<String> path;
        private List<Types.NestedField> prefix;


    }

//    private static List<String> name(IntStream path) {
//
//
//    }
//
//    private static class NestedFieldIterator
}
