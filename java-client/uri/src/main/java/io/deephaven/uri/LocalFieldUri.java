package io.deephaven.uri;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * A local Deephaven field URI.
 *
 * <p>
 * For example, {@code local:///f/my_table}.
 */
@Immutable
@SimpleStyle
public abstract class LocalFieldUri extends LocalUriBase {

    public static final Path FIELD = Paths.get("f");

    public static boolean isMatch(Path path) {
        return !path.isAbsolute() && path.getNameCount() == 2 && FIELD.equals(path.getName(0));
    }

    public static LocalFieldUri of(String fieldName) {
        return ImmutableLocalFieldUri.of(fieldName);
    }

    public static LocalFieldUri of(Path path) {
        if (!isMatch(path)) {
            throw new IllegalArgumentException();
        }
        return of(path.getName(1).toString());
    }

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
     * The path, equivalent to {@code f/${fieldName}}.
     *
     * @return the path.
     */
    @Override
    public final Path localPath() {
        return FIELD.resolve(fieldName());
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
