package io.deephaven.iceberg.layout;

import com.google.common.collect.AbstractIterator;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.NestedField;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class SchemaHelper {

//    public static Stream<NestedField> fieldPath(Schema schema, Iterator<String> iterator) {
//        return StreamSupport.stream(
//                Spliterators.spliteratorUnknownSize(new ItByName(schema, iterator), Spliterator.ORDERED | Spliterator.NONNULL),
//                false);
//    }

    public static Stream<NestedField> nestedFields(Schema schema, PrimitiveIterator.OfInt idPath) {
        if (!idPath.hasNext()) {
            return Stream.empty();
        }
        final int firstId = idPath.nextInt();
        final NestedField firstField = schema.findField(firstId);
        if (firstField == null) {
            throw invalidIdPath(firstId, 0);
        }
        return Stream.concat(
                Stream.of(firstField),
                StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(new ItById(idPath, firstField), Spliterator.ORDERED | Spliterator.NONNULL), false));
    }

//    public static Stream<Type> nestedFields(Schema schema, Iterator<String> namePath) {
//        if (!namePath.hasNext()) {
//            return Stream.empty();
//        }
//        final String firstName = namePath.next();
//        final Type firstField = schema.findType(firstName);
//        if (firstField == null) {
//            throw invalidNamePath(firstName, 0);
//        }
//        schema.findField()
//
//
//
//        return Stream.concat(
//                Stream.of(firstField.type()),
//                StreamSupport.stream(
//                        Spliterators.spliteratorUnknownSize(new ItByName(namePath, fi), Spliterator.ORDERED | Spliterator.NONNULL), false));
//    }
//


    public static void walk(NestedField field, Consumer<NestedField> consumer) {
        if (field.type().isNestedType()) {
            for (NestedField nestedField : field.type().asNestedType().fields()) {
                consumer.accept(nestedField);
            }
        }
    }


    private static class ItById extends AbstractIterator<NestedField> {
        private final PrimitiveIterator.OfInt it;
        private NestedField computed;
        private int computedIx;

        public ItById(PrimitiveIterator.OfInt it, NestedField computed) {
            this.it = Objects.requireNonNull(it);
            this.computed = Objects.requireNonNull(computed);
            this.computedIx = 0;
        }

        @Override
        protected @Nullable NestedField computeNext() {
            if (computed == null || !it.hasNext()) {
                computed = null;
                return endOfData();
            }
            if (!computed.type().isNestedType()) {
                throw new IllegalArgumentException("Invalid path, is too long");
            }
            final int fieldId = it.nextInt();
            computed = computed.type().asNestedType().field(fieldId);
            ++computedIx;
            if (computed == null) {
                throw invalidIdPath(fieldId, computedIx);
            }
            return computed;
        }
    }

    // TODO: special handling for "key", "value", "element"
//    private static class ItByName extends AbstractIterator<Type> {
//        private final Iterator<String> it;
//        private Type computed;
//        private int computedIx;
//
//        public ItByName(Iterator<String> it, Type computed) {
//            this.it = Objects.requireNonNull(it);
//            this.computed = Objects.requireNonNull(computed);
//            this.computedIx = 0;
//        }
//
//        @Override
//        protected @Nullable Type computeNext() {
//            if (computed == null || !it.hasNext()) {
//                computed = null;
//                return endOfData();
//            }
//            if (!computed.isNestedType()) {
//                throw new IllegalArgumentException("Must be nested, illegal path");
//            }
//            final String name = it.next();
//            computed = computed.asNestedType().fieldType(name);
//            ++computedIx;
//            if (computed == null) {
//                throw invalidNamePath(name, computedIx);
//            }
//            return computed;
//        }
//    }

    private static IllegalArgumentException invalidIdPath(int id, int ix) {
        throw new IllegalArgumentException(String.format("Invalid id path, id=%d @ ix=%d", id, ix));
    }

//    private static IllegalArgumentException invalidNamePath(String name, int ix) {
//        throw new IllegalArgumentException(String.format("Invalid name path, name=%s @ ix=%d", name, ix));
//    }
}
