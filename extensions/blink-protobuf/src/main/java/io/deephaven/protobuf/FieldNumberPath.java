package io.deephaven.protobuf;

import com.google.protobuf.Descriptors.FieldDescriptor;
import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.List;

@Immutable
@SimpleStyle
public abstract class FieldNumberPath {

    public static FieldNumberPath of(int[] path) {
        return ImmutableFieldNumberPath.of(path);
    }

    public static FieldNumberPath of(List<FieldDescriptor> fieldDescriptors) {
        final int L = fieldDescriptors.size();
        final int[] path = new int[L];
        for (int i = 0; i < L; ++i) {
            path[i] = fieldDescriptors.get(i).getNumber();
        }
        return of(path);
    }

    @Parameter
    public abstract int[] path();
}
