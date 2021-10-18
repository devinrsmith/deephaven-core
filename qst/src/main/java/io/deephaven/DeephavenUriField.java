package io.deephaven;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.nio.file.Path;

@Immutable
@BuildableStyle
public abstract class DeephavenUriField extends DeephavenUriBase {

    public static Builder builder() {
        return ImmutableDeephavenUriField.builder();
    }

    public static boolean isMatch(Path path) {
        return !path.isAbsolute() && path.getNameCount() == 2 && FIELD.equals(path.getName(0));
    }

    public final String applicationId() {
        return host().get();
    }

    public abstract String fieldName();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final Path path() {
        return FIELD.resolve(fieldName());
    }

    @Check
    final void checkCanUseImplicit() {
        if (!host().isPresent()) {
            throw new IllegalArgumentException("Unable to use implicit application field when no host is present");
        }
    }

    @Check
    final void checkFieldName() {
        // todo
    }

    public interface Builder {

        Builder host(String host);

        Builder port(int port);

        Builder fieldName(String fieldName);

        default Builder parse(Path path) {
            if (!isMatch(path)) {
                throw new IllegalArgumentException();
            }
            return fieldName(path.getName(1).toString());
        }

        DeephavenUriField build();
    }


}
