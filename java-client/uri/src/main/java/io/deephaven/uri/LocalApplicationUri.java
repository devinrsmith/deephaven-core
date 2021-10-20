package io.deephaven.uri;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.nio.file.Path;
import java.nio.file.Paths;

import static io.deephaven.uri.LocalFieldUri.FIELD;

/**
 * A local Deephaven application field URI.
 *
 * <p>
 * For example, {@code local:///a/my_application/f/my_table}.
 */
@Immutable
@SimpleStyle
public abstract class LocalApplicationUri extends LocalUriBase {

    public static final Path APPLICATION = Paths.get("a");

    public static boolean isMatch(Path path) {
        return !path.isAbsolute() && path.getNameCount() == 4 && APPLICATION.equals(path.getName(0))
                && FIELD.equals(path.getName(2));
    }

    public static LocalApplicationUri of(String applicationId, String fieldName) {
        return ImmutableLocalApplicationUri.of(applicationId, fieldName);
    }

    public static LocalApplicationUri of(Path path) {
        if (!isMatch(path)) {
            throw new IllegalArgumentException();
        }
        return of(path.getName(1).toString(), path.getName(3).toString());
    }

    /**
     * The application id.
     *
     * @return the application id
     */
    @Parameter
    public abstract String applicationId();

    /**
     * The field name.
     *
     * @return the field name
     */
    @Parameter
    public abstract String fieldName();

    @Override
    public final <V extends LocalUri.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    /**
     * The path, equivalent to {@code a/${applicationId}/f/${fieldName}}.
     *
     * @return the path.
     */
    @Override
    public final Path localPath() {
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
}
