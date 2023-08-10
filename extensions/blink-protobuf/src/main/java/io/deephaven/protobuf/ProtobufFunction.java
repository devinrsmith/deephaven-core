package io.deephaven.protobuf;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.deephaven.annotations.SimpleStyle;
import io.deephaven.stream.blink.tf.TypedFunction;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.List;
import java.util.stream.Collectors;

@Immutable
@SimpleStyle
public abstract class ProtobufFunction {

    public static List<String> toNamedPath(List<FieldDescriptor> fieldDescriptors) {
        return fieldDescriptors.stream().map(FieldDescriptor::getName).collect(Collectors.toList());
    }

    public static ProtobufFunction of(TypedFunction<Message> f) {
        return of(List.of(), f);
    }

    public static ProtobufFunction of(List<FieldDescriptor> path, TypedFunction<Message> f) {
        return ImmutableProtobufFunction.of(path, f);
    }

    @Parameter
    public abstract List<FieldDescriptor> path();

    @Parameter
    public abstract TypedFunction<Message> function();

    public final boolean matches(int... fieldNumberPath) {
        final int L = fieldNumberPath.length;
        if (path().size() != L) {
            return false;
        }
        for (int i = 0; i < L; ++i) {
            if (path().get(i).getNumber() != fieldNumberPath[i]) {
                return false;
            }
        }
        return true;
    }

    public final boolean matches(String... namePath) {
        final int L = namePath.length;
        if (path().size() != L) {
            return false;
        }
        for (int i = 0; i < L; ++i) {
            if (!path().get(i).getName().equals(namePath[i])) {
                return false;
            }
        }
        return true;
    }

    public final int[] fieldNumberPath() {
        final int L = path().size();
        final int[] fieldNumberPath = new int[L];
        for (int i = 0; i < L; ++i) {
            fieldNumberPath[i] = path().get(i).getNumber();
        }
        return fieldNumberPath;
    }
}
