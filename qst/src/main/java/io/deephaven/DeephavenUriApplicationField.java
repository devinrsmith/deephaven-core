package io.deephaven;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.nio.file.Path;

@Immutable
@BuildableStyle
public abstract class DeephavenUriApplicationField extends DeephavenUriBase {

    public static Builder builder() {
        return ImmutableDeephavenUriApplicationField.builder();
    }

    public static boolean isMatch(Path path) {
        return !path.isAbsolute() && path.getNameCount() == 4 && APPLICATION.equals(path.getName(0))
                && FIELD.equals(path.getName(2));
    }

    public abstract String applicationId();

    public abstract String fieldName();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final Path path() {
        return APPLICATION.resolve(applicationId()).resolve(FIELD).resolve(fieldName());
    }

    @Check
    final void checkApplicationId() {
        // todo: check app id
    }

    @Check
    final void checkFieldName() {
        // todo
    }

    public interface Builder {

        Builder target(DeephavenTarget target);

        Builder applicationId(String applicationId);

        Builder fieldName(String fieldName);

        default Builder parse(Path path) {
            if (!isMatch(path)) {
                throw new IllegalArgumentException();
            }
            return applicationId(path.getName(1).toString()).fieldName(path.getName(3).toString());
        }

        DeephavenUriApplicationField build();
    }
}
