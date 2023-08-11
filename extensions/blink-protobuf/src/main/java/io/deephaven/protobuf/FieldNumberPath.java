package io.deephaven.protobuf;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.Arrays;

@Immutable
@SimpleStyle
public abstract class FieldNumberPath {

    public static FieldNumberPath of(int... path) {
        return ImmutableFieldNumberPath.of(path);
    }

    @Parameter
    public abstract int[] path();

    public final boolean startsWith(FieldNumberPath other) {
        final int[] otherPath = other.path();
        return Arrays.equals(path(), 0, Math.min(path().length, otherPath.length), otherPath, 0, otherPath.length);
    }
}
