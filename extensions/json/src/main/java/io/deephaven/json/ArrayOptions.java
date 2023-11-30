/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.Type;
import io.deephaven.qst.type.Type.Visitor;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class ArrayOptions extends ValueOptions {

    public static Builder builder() {
        return null;
    }

    public abstract ValueOptions element();

    @Override
    @Default
    public boolean allowNull() {
        return true;
    }

    @Override
    @Default
    public boolean allowMissing() {
        return true;
    }

    final boolean asArray() {
        // todo, expose to user, provide option to use ChunkProvider (ie, multiple rows)
        return true;
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
    final Map<JsonToken, JsonToken> startEndTokens() {
        return Map.of(JsonToken.START_ARRAY, JsonToken.END_ARRAY);
    }

    @Override
    final ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        if (element().numColumns() != 1) {
            throw new IllegalArgumentException("Need multivariate (ChunkProvider) support for this");
        }
        if (!asArray()) {
            throw new IllegalArgumentException("Need multivariate (ChunkProvider) support for this");
        }
        return element().outputTypes().findFirst().orElseThrow().walk(new Visitor<ValueProcessor>() {
            @Override
            public ValueProcessor visit(PrimitiveType<?> primitiveType) {
                return null;
            }

            @Override
            public ValueProcessor visit(GenericType<?> genericType) {
                return null;
            }
        });



        // todo: option to do multivariate w/ single value
        final Type<?> elementType = element().outputTypes().findFirst().orElseThrow();
        if (Type.intType().equals(elementType)) {
            return new IntArrayChunkValueProcessor(context, allowNull(), allowMissing(), null, true);
        }


    }

    public interface Builder extends ValueOptions.Builder<ArrayOptions, Builder> {

    }
}
