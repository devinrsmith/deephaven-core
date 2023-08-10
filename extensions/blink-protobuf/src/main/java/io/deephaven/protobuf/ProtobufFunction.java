package io.deephaven.protobuf;

import com.google.protobuf.Message;
import io.deephaven.annotations.SimpleStyle;
import io.deephaven.stream.blink.tf.TypedFunction;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
public abstract class ProtobufFunction {

    public static ProtobufFunction of(TypedFunction<Message> f) {
        return of(FieldPath.empty(), f);
    }

    public static ProtobufFunction of(FieldPath path, TypedFunction<Message> f) {
        return ImmutableProtobufFunction.of(path, f);
    }

    @Parameter
    public abstract FieldPath path();

    @Parameter
    public abstract TypedFunction<Message> function();
}
