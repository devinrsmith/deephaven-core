//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.iceberg.internal.Inference;
import io.deephaven.iceberg.internal.SchemaHelper;
import io.deephaven.qst.type.Type;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.NestedField;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


// This is a mapping for _read_
@Value.Immutable
@BuildableStyle
public abstract class DefinitionInstructions {

    public static Builder builder() {
        return ImmutableDefinitionInstructions.builder();
    }

    public static DefinitionInstructions empty(Schema schema) {
        return builder()
                .schema(schema)
                .definition(TableDefinition.of(List.of()))
                .build();
    }

    /**
     * Infer a table definition instructions based on the {@code schema}.
     */
    public static DefinitionInstructions infer(Schema schema) {
        // TODO: NEED TO HANDLE PARTITIONING
        final InferenceBuilder builder = new InferenceBuilder(false);
        Inference.of(schema, builder);
        return builder.build(schema);
    }

    public static DefinitionInstructions inferAll(Schema schema) throws Inference.Exception {
        final InferenceBuilder builder = new InferenceBuilder(true);
        try {
            Inference.of(schema, builder);
        } catch (RuntimeException e) {
            throw extract(e);
        }
        return builder.build(schema);
    }

    // public static DefinitionInstructions inferUpdate(DefinitionInstructions existingInstructions, Schema newSchema) {
    // // TODO
    // return null;
    // }

    /**
     * The Iceberg schema. This schema is set at the time these instructions were originally made - it is often
     * <b>not</b> the most recent schema for an Iceberg table.
     */
    public abstract Schema schema();

    /**
     * The Deephaven table definition.
     */
    public abstract TableDefinition definition();

    /**
     * The column instructions keyed by Deephaven column name.
     */
    public abstract Map<String, ColumnInstructions> columnInstructions();

    // We need to store as List so we can get test out the mapping wrt equals
    // todo: verify this is pointing to a schema leaf; arguably, doesn't
    // have to be a leaf if it's a list? b/c really, the list type is
    // probably better than a path to the elemnt

    @Value.Default
    boolean allowUnmappedColumns() {
        return false;
    }

    // todo: need to specify any special transformations that happen


    public interface Builder {

        Builder schema(Schema schema);

        Builder definition(TableDefinition definition);

        Builder putColumnInstructions(String columnName, ColumnInstructions columnInstructions);

        default Builder putColumnInstructions(String columnName, FieldPath fieldPath) {
            return putColumnInstructions(columnName, ColumnInstructions.of(fieldPath));
        }

        Builder allowUnmappedColumns(boolean allowUnmappedColumns);

        DefinitionInstructions build();
    }

    @Value.Check
    final void checkUnmappedColumns() {
        if (allowUnmappedColumns()) {
            return;
        }
        for (String column : definition().getColumnNameSet()) {
            if (!columnInstructions().containsKey(column)) {
                throw new MappingException(String.format("Column `%s` is not mapped", column));
            }
        }
    }

    @Value.Check
    final void checkCompatibility() {
        for (Map.Entry<String, ColumnInstructions> e : columnInstructions().entrySet()) {
            definition().checkHasColumn(e.getKey());
            final ColumnDefinition<?> column = definition().getColumn(e.getKey());
            final ColumnInstructions columnInstructions = e.getValue();
            final List<NestedField> fieldPath = columnInstructions.path().resolve(schema());
            // TODO: we need to make this official
            final Type<?> type = Type.find(column.getDataType());
            checkCompatible(fieldPath, type);
        }
    }

    // TODO
    public final ResolverFactory factory() {
        return new ResolverFactory(this);
    }

    private static class InferenceBuilder implements Inference.Consumer {
        private final Set<String> usedNames = new HashSet<>();
        private final List<ColumnDefinition<?>> definitions = new ArrayList<>();
        private final Builder builder = builder();
        private final boolean throwOnError;

        public InferenceBuilder(boolean throwOnError) {
            this.throwOnError = throwOnError;
        }

        DefinitionInstructions build(Schema schema) {
            return builder
                    .schema(schema)
                    .definition(TableDefinition.of(definitions))
                    .allowUnmappedColumns(false)
                    .build();
        }

        @Override
        public void onType(Collection<? extends NestedField> path, Type<?> type) {
            if (!isCompatible(path, type)) {
                throw new IllegalStateException(
                        String.format("Inference is producing an invalid mapping path=%s, type=%s",
                                SchemaHelper.toNameString(path), type));
            }
            final String joinedNames = path.stream().map(NestedField::name).collect(Collectors.joining("_"));
            final String columnName = NameValidator.legalizeColumnName(joinedNames, usedNames);
            final int[] idPath = path.stream().mapToInt(NestedField::fieldId).toArray();
            builder.putColumnInstructions(columnName, ColumnInstructions.of(FieldPath.of(idPath)));
            definitions.add(ColumnDefinition.of(columnName, type));
            usedNames.add(columnName);
        }

        @Override
        public void onError(Collection<? extends NestedField> path, Inference.Exception e) {
            if (throwOnError) {
                throw new RuntimeException(e);
            }
        }
    }

    static boolean isCompatible(Collection<? extends NestedField> path, Type<?> type) {
        try {
            checkCompatible(path, type);
            return true;
        } catch (MappingException e) {
            return false;
        }
    }


    static void checkCompatible(Collection<? extends NestedField> path, Type<?> type) {
        // We are assuming that fieldPath has been properly constructed from a Schema. This makes it a poor candidate
        // as public API.
        checkCompatible(path);
        // todo: compare against DH type(s)
        final NestedField lastField = path.stream().reduce((p, n) -> n).orElseThrow();
        if (!type.walk(new IcebergPrimitiveCompat(lastField.type().asPrimitiveType()))) {
            throw new MappingException(
                    String.format("Unable to map Iceberg type `%s` to Deephaven type `%s`", lastField.type(), type));
        }
    }

    static void checkCompatible(Collection<? extends NestedField> fieldPath) {
        // We are assuming that fieldPath has been properly constructed from a Schema. This makes it a poor candidate
        // as public API.
        if (fieldPath.isEmpty()) {
            throw new MappingException("Can't map an empty field path");
        }

        // We should make sure that we standardize on the correct level of mapping. For example, if we eventually
        // support List<Primitive>, we should make sure the fieldPath points to the *List* instead of the primitive, as
        // it's a better representation of the desired DH type. Although, we should arguably be able to support
        // List<Struct<A, ..., Z>> materialized into column types A[], ..., Z[], and that likely needs the more specific
        // field path into the actual primitive type... TBD.
        //
        // We also need to determine with how much precision we can or want to preserve optional semantics. For example,
        // a required struct with an optional field, or an optional struct with a required field are easy to represent
        // as a DH column; null implies that the optional aspect of that value was not present. But, if there is an
        // optional struct with an optional field, we are unable to whether null means the struct was not present, or
        // the field was not present. We could consider a special mapping (likely a `byte` column) that could be relayed
        // that lets the user know at which level the value resolved to:
        //
        // "MyFoo" -> Struct/Foo
        // "MyFooLevel" -> level(Struct/Foo)
        //
        // level 0 means the struct was not present
        // level 1 means the value was not present
        //
        // or, we could have a Boolean that represents whether path was present, and user would be build up logic as
        // necessary:
        //
        // "MyFoo" -> Struct1/Struct2/Foo
        // "MyBar" -> Struct1/Struct2/Bar
        // "Struct1Present" -> present(Struct1)
        // "Struct2Present" -> present(Struct1/Struct2) (would be null if Struct1 is not present)

        NestedField lastField = null;
        for (final NestedField nestedField : fieldPath) {
            if (nestedField.type().isListType()) {
                throw new MappingException("List type not supported"); // todo better error
            }
            if (nestedField.type().isMapType()) {
                throw new MappingException("Map type not supported"); // todo better error
            }
            // We *do* support struct mapping implicitly.
            lastField = nestedField;
        }
        if (!lastField.type().isPrimitiveType()) {
            // This could be extended in the future with support for:
            // * List<Primitive>
            // * List<List<...<Primitive>...>
            // * Map<Primitive, Primitive>
            // * Map<List<Primitive>, List<Primitive>>
            // * Struct<...> (if DH theoretically allowed struct columns, unlikely but possible)
            // * Other combinations of above.
            throw new MappingException("Only support mapping to primitive types " + lastField.type());
        }
        checkCompatible(lastField.type().asPrimitiveType());
    }

    static void checkCompatible(org.apache.iceberg.types.Type.PrimitiveType type) {
        // do we even support this type? note: it's _possible_ there are cases where there is a primitive type where
        // we don't support inference, but do support compatibility with DH type... TODO
        try {
            Inference.of(type);
        } catch (Inference.UnsupportedType e) {
            throw new MappingException(e.getMessage());
        }
    }

    public static class MappingException extends RuntimeException {

        public MappingException(String message) {
            super(message);
        }
    }

    private static RuntimeException extract(RuntimeException e) throws Inference.Exception {
        if (e.getCause() instanceof Inference.Exception) {
            throw ((Inference.Exception) e.getCause());
        }
        throw e;
    }
}
