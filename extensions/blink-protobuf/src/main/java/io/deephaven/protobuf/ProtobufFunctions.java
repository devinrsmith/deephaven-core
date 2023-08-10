package io.deephaven.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.stream.blink.tf.TypedFunction;
import org.immutables.value.Value.Immutable;

import java.util.List;
import java.util.Optional;

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
        return new Protobuf(options).translate(descriptor);
    }

    public static ProtobufFunctions unnamed(TypedFunction<Message> tf) {
        return builder().addFunctions(ProtobufFunction.of(tf)).build();
    }


    public abstract List<ProtobufFunction> functions();

    public final Optional<ProtobufFunction> find(int... fieldNumberPath) {
        for (ProtobufFunction function : functions()) {
            if (function.matches(fieldNumberPath)) {
                return Optional.of(function);
            }
        }
        return Optional.empty();
    }

    public final Optional<ProtobufFunction> find(String... namePath) {
        for (ProtobufFunction function : functions()) {
            if (function.matches(namePath)) {
                return Optional.of(function);
            }
        }
        return Optional.empty();
    }

    public interface Builder {
        Builder addFunctions(ProtobufFunction element);

        Builder addFunctions(ProtobufFunction... elements);

        Builder addAllFunctions(Iterable<? extends ProtobufFunction> elements);

        ProtobufFunctions build();
    }
}
