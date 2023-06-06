package io.deephaven.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.stream.blink.tf.TypedFunction;
import org.immutables.value.Value.Immutable;

import java.util.List;
import java.util.Map;

@Immutable
@BuildableStyle
public abstract class ProtobufFunctions {

    public static Builder builder() {
        return ImmutableProtobufFunctions.builder();
    }

    public static ProtobufFunctions empty() {
        return builder().build();
    }

    public static ProtobufFunctions parse(Descriptor descriptor, ProtobufOptions options) {
        return new Protobuf2(options).translate(descriptor);
    }

    public static ProtobufFunctions unnamed(TypedFunction<Message> tf) {
        return builder().putColumns(List.of(), tf).build();
    }

    public abstract Map<List<String>, TypedFunction<Message>> columns();

    public interface Builder {

        Builder putColumns(List<String> key, TypedFunction<Message> value);

        Builder putAllColumns(Map<? extends List<String>, ? extends TypedFunction<Message>> entries);

        ProtobufFunctions build();
    }
}
