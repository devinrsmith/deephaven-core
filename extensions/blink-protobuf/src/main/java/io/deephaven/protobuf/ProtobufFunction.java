package io.deephaven.protobuf;

import com.google.protobuf.Message;
import io.deephaven.annotations.SimpleStyle;
import io.deephaven.stream.blink.tf.TypedFunction;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * Encapsulates the logic to extract a result from a {@link Message}.
 */
@Immutable
@SimpleStyle
public abstract class ProtobufFunction {

    public static ProtobufFunction of(TypedFunction<Message> f) {
        return of(FieldPath.empty(), f);
    }

    public static ProtobufFunction of(FieldPath path, TypedFunction<Message> f) {
        return ImmutableProtobufFunction.of(path, f);
    }

    /**
     * The path that {@link #function()} uses to produce its result.
     *
     * @return the path
     */
    @Parameter
    public abstract FieldPath path();

    /**
     * The function to extract a result from a {@link Message}.
     *
     * @return the function
     */
    @Parameter
    public abstract TypedFunction<Message> function();
}
