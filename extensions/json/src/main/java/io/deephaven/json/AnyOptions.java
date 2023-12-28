/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import io.deephaven.annotations.SimpleStyle;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.ObjectValueProcessor.ToObject;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Immutable;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

/**
 * Note: TreeNode should not be considered part of the public API. Implementation may change.
 */
@Immutable
@SimpleStyle
public abstract class AnyOptions extends ValueOptions {

    public static AnyOptions of() {
        return ImmutableAnyOptions.of();
    }

    @Override
    public final boolean allowNull() {
        return true;
    }

    @Override
    public final boolean allowMissing() {
        return true;
    }

    @Override
    Stream<Type<?>> outputTypes() {
        return Stream.of(Type.ofCustom(TreeNode.class));
    }

    @Override
    ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new ObjectValueProcessor<>(out.get(0).asWritableObjectChunk(), ToTreeNode.INSTANCE);
    }

    private enum ToTreeNode implements ToObject<TreeNode> {
        INSTANCE;

        @Override
        public TreeNode parseValue(JsonParser parser) throws IOException {
            return parser.readValueAsTree();
        }

        @Override
        public TreeNode parseMissing(JsonParser parser) {
            return MissingNode.getInstance();
        }
    }
}
