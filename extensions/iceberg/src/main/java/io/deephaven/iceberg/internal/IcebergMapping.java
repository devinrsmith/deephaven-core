package io.deephaven.iceberg.internal;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.iceberg.layout.SchemaHelper;
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

@Value.Immutable
@BuildableStyle
public abstract class IcebergMapping {

    public static Builder builder() {
        return ImmutableIcebergMapping.builder();
    }

    public static IcebergMapping infer(Schema schema) {
        final MapViaInference mapViaInference = new MapViaInference(false);
        Inference.of(schema, mapViaInference);
        return mapViaInference.build(schema);
    }

    public static IcebergMapping inferAll(Schema schema) throws Inference.Exception {
        final MapViaInference mapViaInference = new MapViaInference(true);
        try {
            Inference.of(schema, mapViaInference);
        } catch (RuntimeException e) {
            if (e.getCause() instanceof Inference.Exception) {
                throw ((Inference.Exception) e.getCause());
            }
            throw e;
        }
        return mapViaInference.build(schema);
    }

    private static class MapViaInference implements Inference.Consumer {
        private final Set<String> usedNames = new HashSet<>();
        private final List<ColumnDefinition<?>> definitions = new ArrayList<>();
        private final Builder builder = builder();
        private final boolean throwOnError;

        public MapViaInference(boolean throwOnError) {
            this.throwOnError = throwOnError;
        }

        IcebergMapping build(Schema schema) {
            return builder
                    .schema(schema)
                    .definition(TableDefinition.of(definitions))
                    .allowUnmappedColumns(false)
                    .build();
        }

        @Override
        public void onType(Collection<? extends NestedField> path, Type<?> type) {
            final String joinedNames = path.stream().map(NestedField::name).collect(Collectors.joining("_"));
            final String columnName = NameValidator.legalizeColumnName(joinedNames, usedNames);
            final int[] idPath = path.stream().mapToInt(NestedField::fieldId).toArray();
            builder.putMap(columnName, idPath);
            definitions.add(ColumnDefinition.of(columnName, type));
        }

        @Override
        public void onError(Collection<? extends NestedField> path, Inference.Exception e) {
            if (throwOnError) {
                throw new RuntimeException(e);
            }
        }
    }

    static void checkCompatible(ColumnDefinition<?> columnDefinition, List<NestedField> fieldPath) {
        // We are assuming that fieldPath has been properly constructed from a Schema. This makes it a poor candidate
        // as public API.
        checkCompatible(fieldPath);
        // todo: compare against DH type(s)
    }

    static void checkCompatible(List<NestedField> fieldPath) {
        // We are assuming that fieldPath has been properly constructed from a Schema. This makes it a poor candidate
        // as public API.
        if (fieldPath.isEmpty()) {
            throw new MappingE("Can't map an empty field path");
        }
        final NestedField lastField = fieldPath.get(fieldPath.size() - 1);
        if (!lastField.type().isPrimitiveType()) {
            // This could be extended in the future with support for:
            // * List<Primitive>
            // * List<List<...<Primitive>...>
            // * Map<Primitive, Primitive>
            // * Map<List<Primitive>, List<Primitive>>
            // * Struct<...> (if DH theoretically allowed struct columns, unlikely but possible)
            // * Other combinations of above.
            throw new MappingE("Only support mapping to primitive types");
        }
        checkCompatible(lastField.type().asPrimitiveType());
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
        for (final NestedField nestedField : fieldPath) {
            if (nestedField.type().isListType()) {
                throw new MappingE("Intermediate List type not supported");
            }
            if (nestedField.type().isMapType()) {
                throw new MappingE("Intermediate Map type not supported");
            }
            // We *do* support struct mapping implicitly.
        }
    }

    static void checkCompatible(org.apache.iceberg.types.Type.PrimitiveType type) {
        // do we even support this type? note: it's _possible_ there are cases where there is a primitive type where
        // we don't support inference, but do support compatibility with DH type... TODO
        try {
            Inference.of(type);
        } catch (Inference.UnsupportedType e) {
            throw new MappingE(e.getMessage());
        }
    }

    public abstract Schema schema();

    public abstract TableDefinition definition();

    @Value.Default
    public boolean allowUnmappedColumns() {
        return false;
    }

    // does not support mapping to a key / value of a map type
    abstract Map<String, int[]> map(); // todo: verify this is pointing to a schema leaf; arguably, doesn't have to be a leaf if it's a list? b/c really, the list type is probably better than a path to the elemnt

    // todo: need to specify any special transformations that happen

    static class MappingE extends RuntimeException {

        public MappingE(String message) {
            super(message);
        }
    }

    public interface Builder {

        Builder definition(TableDefinition definition);

        Builder schema(Schema schema);

        Builder putMap(String deephavenColumnName, int[] value);

        Builder allowUnmappedColumns(boolean allowUnmappedColumns);

        IcebergMapping build();
    }

    @Value.Check
    final void checkUnmappedColumns() {
        if (allowUnmappedColumns()) {
            return;
        }
        for (String column : definition().getColumnNameSet()) {
            if (!map().containsKey(column)) {
                throw new RuntimeException("todo");
            }
        }
    }

    @Value.Check
    final void checkCompatibility() {
        for (Map.Entry<String, int[]> e : map().entrySet()) {
            definition().checkHasColumn(e.getKey());
            final ColumnDefinition<?> column = definition().getColumn(e.getKey());
            final List<NestedField> fieldPath = SchemaHelper.fieldPath(schema(), e.getValue());
            checkCompatible(column, fieldPath);
        }
    }
}
