package io.deephaven;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

import java.nio.file.Path;

@Immutable
@BuildableStyle
public abstract class DeephavenUriQueryScope extends DeephavenUriBase {

    public static Builder builder() {
        return ImmutableDeephavenUriQueryScope.builder();
    }

    public static boolean isMatch(Path path) {
        return !path.isAbsolute() && path.getNameCount() == 2 && QUERY_SCOPE.equals(path.getName(0));
    }

    public abstract String variableName();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final Path path() {
        return QUERY_SCOPE.resolve(variableName());
    }

    public interface Builder {

        Builder host(String host);

        Builder port(int port);

        Builder variableName(String variableName);

        default Builder parse(Path path) {
            if (!isMatch(path)) {
                throw new IllegalArgumentException();
            }
            return variableName(path.getName(1).toString());
        }

        DeephavenUriQueryScope build();
    }
}
