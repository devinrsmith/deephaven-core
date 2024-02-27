/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.AnyOptions;
import io.deephaven.json.jackson.ObjectValueProcessor.ToObject;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

final class AnyMixin extends Mixin<AnyOptions> {
    public AnyMixin(AnyOptions options, JsonFactory factory) {
        super(factory, options);
    }

    // TODO: ALLOW NULL USAGE?

    @Override
    public int outputCount() {
        return 1;
    }

    @Override
    public Stream<List<String>> paths() {
        return Stream.of(List.of());
    }

    @Override
    public Stream<Type<?>> outputTypes() {
        return Stream.of(Type.ofCustom(TreeNode.class));
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
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
            return parser.getCodec().missingNode();
        }
    }
}
