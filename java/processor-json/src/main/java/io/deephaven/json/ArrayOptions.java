/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.jackson.ValueProcessor;
import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.StringType;
import io.deephaven.qst.type.Type;
import io.deephaven.qst.type.Type.Visitor;
import org.immutables.value.Value.Immutable;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class ArrayOptions extends ValueOptions {

    public static Builder builder() {
        return null;
        // return ImmutableArrayOptions.builder();
    }

    public abstract ValueOptions element();

    final boolean asArray() {
        // todo, expose to user, provide option to use ChunkProvider (ie, multiple rows)
        return true;
    }

    @Override
    final int outputCount() {
        return element().outputCount();
    }

    @Override
    final Stream<List<String>> paths() {
        throw new UnsupportedOperationException();
    }

    @Override
    final Stream<Type<?>> outputTypes() {
        if (element().numColumns() != 1) {
            throw new IllegalArgumentException("Need multivariate (ChunkProvider) support for this");
        }
        if (!asArray()) {
            throw new IllegalArgumentException("Need multivariate (ChunkProvider) support for this");
        }
        return element().outputTypes().map(Type::arrayType);
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        if (element().numColumns() != 1) {
            throw new IllegalArgumentException("Need multivariate (ChunkProvider) support for this");
        }


        // if (element() instanceof ObjectOptions) {
        // final ObjectOptions elementObject = (ObjectOptions) element();
        // final ObjectOptions.Builder builder = ObjectOptions.builder()
        // .allowNull(allowNull())
        // .allowMissing(allowMissing());
        // for (Entry<String, ValueOptions> e : elementObject.fieldProcessors().entrySet()) {
        // builder.putFieldProcessors(e.getKey(), e.getValue().toArrayOptions());
        // }
        // return builder.build().processor(context, out);
        // }


        // todo: we should be abel to work w/ ObjectOptions and do arrays for each individual type
        if (!asArray()) {
            throw new IllegalArgumentException("Need multivariate (ChunkProvider) support for this");
        }
        return element().outputTypes().findFirst().orElseThrow().walk(new What(context, out));
    }

    public interface Builder extends ValueOptions.Builder<ArrayOptions, Builder> {

        Builder element(ValueOptions options);
    }

    private class What implements Visitor<ValueProcessor>, PrimitiveType.Visitor<ValueProcessor>,
            GenericType.Visitor<ValueProcessor> {
        private final String context;
        private final List<WritableChunk<?>> out;

        What(String context, List<WritableChunk<?>> out) {
            this.context = Objects.requireNonNull(context);
            this.out = Objects.requireNonNull(out);
        }

        @Override
        public ValueProcessor visit(PrimitiveType<?> primitiveType) {
            return primitiveType.walk((PrimitiveType.Visitor<ValueProcessor>) this);
        }

        @Override
        public ValueProcessor visit(GenericType<?> genericType) {
            return genericType.walk((GenericType.Visitor<ValueProcessor>) this);
        }

        @Override
        public ValueProcessor visit(BooleanType booleanType) {
            return null;
        }

        @Override
        public ValueProcessor visit(ByteType byteType) {
            return null;
        }

        @Override
        public ValueProcessor visit(CharType charType) {
            return null;
        }

        @Override
        public ValueProcessor visit(ShortType shortType) {
            return null;
        }

        @Override
        public ValueProcessor visit(IntType intType) {
            return null;
            // return new IntArrayChunkValueProcessor(
            // context, allowNull(), allowMissing(), out.get(0).asWritableObjectChunk(), element().allowNull());
        }

        @Override
        public ValueProcessor visit(LongType longType) {
            return null;
        }

        @Override
        public ValueProcessor visit(FloatType floatType) {
            return null;
        }

        @Override
        public ValueProcessor visit(DoubleType doubleType) {
            return null;
            // return new DoubleArrayChunkValueProcessor(
            // context, allowNull(), allowMissing(), out.get(0).asWritableObjectChunk(), element().allowNull());
        }

        @Override
        public ValueProcessor visit(BoxedType<?> boxedType) {
            return null;
        }

        @Override
        public ValueProcessor visit(StringType stringType) {
            return null;
        }

        @Override
        public ValueProcessor visit(InstantType instantType) {
            return null;
        }

        @Override
        public ValueProcessor visit(ArrayType<?, ?> arrayType) {
            return null;
        }

        @Override
        public ValueProcessor visit(CustomType<?> customType) {
            return null;
        }
    }
}
