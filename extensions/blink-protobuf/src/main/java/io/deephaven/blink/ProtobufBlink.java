package io.deephaven.blink;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.protobuf.Protobuf;
import io.deephaven.protobuf.ProtobufOptions;
import io.deephaven.stream.blink.BlinkTableMapper;
import io.deephaven.stream.blink.BlinkTableMapperConfig;
import io.deephaven.stream.blink.BlinkTableMapperConfig.Builder;
import io.deephaven.stream.blink.tf.TypedFunction;

import java.util.List;
import java.util.Map.Entry;

public class ProtobufBlink {
    public static <M extends Message> BlinkTableMapper<M> create(Descriptor descriptor, ProtobufOptions options) {
        final Builder<M> builder = BlinkTableMapperConfig.<M>builder()
                .name(descriptor.getFullName())
                .chunkSize(1024)
                .updateSourceRegistrar(ExecutionContext.getContext().getUpdateGraph());
        for (Entry<List<String>, TypedFunction<Message>> e : Protobuf.parser(descriptor).parser(options).entrySet()) {
            // noinspection unchecked
            builder.putColumns(pathToName(e.getKey()), (TypedFunction<M>) e.getValue());
        }
        return BlinkTableMapper.create(builder.build());
    }

    private static String pathToName(List<String> path) {
        // todo make option
        return String.join("_", path);
    }
}
