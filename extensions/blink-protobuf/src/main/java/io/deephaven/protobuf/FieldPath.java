package io.deephaven.protobuf;

import com.google.protobuf.Descriptors.FieldDescriptor;
import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Lazy;
import org.immutables.value.Value.Parameter;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Immutable
@SimpleStyle
public abstract class FieldPath {

    public static FieldPath empty() {
        return of(List.of());
    }

    public static FieldPath of(FieldDescriptor... descriptors) {
        return of(Arrays.asList(descriptors));
    }

    public static FieldPath of(List<FieldDescriptor> descriptors) {
        return ImmutableFieldPath.of(descriptors);
    }

    @Parameter
    public abstract List<FieldDescriptor> path();

    @Lazy
    public FieldNumberPath numberPath() {
        return FieldNumberPath.of(path());
    }

    @Lazy
    public List<String> namePath() {
        return path().stream().map(FieldDescriptor::getName).collect(Collectors.toList());
    }

    public final boolean namePathMatches(String... namePath) {
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
}
